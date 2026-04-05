from __future__ import annotations

import contextlib
import tempfile
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

from ankitron.enums import FieldRule, MediaFormat, MediaType, Severity
from ankitron.logging import log_info, log_success, log_warn
from ankitron.transform import Transform, apply_transform_chain


def _process_media_fields(
    cls: type,
    all_rows: list[dict[str, Any]],
    pk_field_attr: str,
) -> None:
    """Process media fields: download URLs, convert to target format, cache, and generate tags."""
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from pathlib import Path

    from ankitron.media.pipeline import (
        convert_image,
        download_media,
        generate_media_filename,
        make_img_tag,
    )

    # Identify media fields
    media_fields = [(name, fld) for name, fld in cls._all_fields if fld.media is not None]

    if not media_fields:
        return

    # Get cache directory
    cache_dir = Path.home() / ".cache" / "ankitron" / "media"
    cache_dir.mkdir(parents=True, exist_ok=True)

    for attr_name, fld in media_fields:
        if fld.media != MediaType.IMAGE:
            # Skip audio and other non-image media for now
            continue

        media_dir = cache_dir
        target_format = fld.format or MediaFormat.PNG

        tasks: list[tuple[int, str, Any, Path, str]] = []
        for row_idx, row in enumerate(all_rows):
            url = row.get(attr_name, "")
            if not url or not isinstance(url, str):
                continue
            if not url.startswith("http"):
                continue

            pk_val = row.get(f"_pk_{pk_field_attr}", row.get(pk_field_attr, ""))
            ext = fld.format.value if fld.format else "png"
            filename = generate_media_filename(cls.__name__, pk_val, attr_name, ext)
            output_path = media_dir / filename
            img_tag = make_img_tag(filename, width=fld.width, height=fld.height)
            tasks.append((row_idx, url, pk_val, output_path, img_tag))

        if not tasks:
            continue

        # Fast path: already cached
        for row_idx, _, _, output_path, img_tag in tasks:
            if output_path.exists():
                all_rows[row_idx][attr_name] = img_tag

        uncached = [t for t in tasks if not t[3].exists()]
        if not uncached:
            continue

        log_info(f"  Processing {len(uncached)} {attr_name} media file(s) in parallel...")

        def _process_one(
            task: tuple[int, str, Any, Path, str],
            *,
            _target_format: Any = target_format,
            _fld: Any = fld,
            _attr_name: str = attr_name,
        ) -> tuple[int, str | None]:
            row_idx, url, pk_val, output_path, _img_tag = task
            try:
                # Download to temporary location with proper suffix for format detection
                import requests

                resp = requests.head(
                    url,
                    timeout=10,
                    allow_redirects=True,
                    headers={"User-Agent": "ankitron/0.1.0"},
                )
                content_type = resp.headers.get("content-type", "").lower()

                # Determine source format from content type
                if "svg" in content_type:
                    tmp_suffix = ".svg"
                elif "jpeg" in content_type or "jpg" in content_type:
                    tmp_suffix = ".jpg"
                elif "png" in content_type:
                    tmp_suffix = ".png"
                elif "webp" in content_type:
                    tmp_suffix = ".webp"
                else:
                    tmp_suffix = ".svg" if ".svg" in url.lower() else ".png"

                with tempfile.NamedTemporaryFile(suffix=tmp_suffix, delete=False) as tmp:
                    tmp_path = Path(tmp.name)

                try:
                    download_media(url, tmp_path, timeout=30)
                    convert_image(
                        input_path=tmp_path,
                        output_path=output_path,
                        target_format=_target_format,
                        width=_fld.width,
                        height=_fld.height,
                    )
                    return (row_idx, None)
                finally:
                    if tmp_path.exists():
                        tmp_path.unlink()
            except Exception as exc:
                return (row_idx, f"  Failed to process {_attr_name} for {pk_val}: {exc}")

        max_workers = min(32, (os.cpu_count() or 4) * 4, len(uncached))
        row_to_tag = {row_idx: img_tag for row_idx, _, _, _, img_tag in uncached}
        future_to_task: dict[Any, tuple[int, str, Any, Path, str]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for task in uncached:
                future_to_task[executor.submit(_process_one, task)] = task

            for future in as_completed(future_to_task):
                row_idx, err = future.result()
                if err is not None:
                    log_warn(err)
                    continue
                all_rows[row_idx][attr_name] = row_to_tag[row_idx]


def _coerce_numeric(value: Any) -> Any:
    """Try to convert a string value to int or float, returning original if not possible."""
    if isinstance(value, str) and value:
        try:
            n = float(value)
            return int(n) if n == int(n) else n
        except (ValueError, TypeError):
            pass
    return value


def _build_transform_steps_for_prov(
    transform: Any,
    input_val: Any,
    output_val: Any,
) -> list:
    """Decompose a Transform (possibly chained) into TransformStep list for provenance."""
    from ankitron.provenance import TransformStep
    from ankitron.transform import ChainedTransform, DatasetAwareTransform

    if isinstance(transform, ChainedTransform):
        steps_out: list = []
        cur = input_val
        for step in transform.steps:
            if isinstance(step, DatasetAwareTransform):
                out = None
            else:
                try:
                    out = step.apply(cur)
                except Exception:
                    out = None
            steps_out.append(
                TransformStep(
                    name=step.name,
                    description=step.description,
                    input_value=cur,
                    output_value=out,
                )
            )
            if out is not None:
                cur = out
        return steps_out
    return [
        TransformStep(
            name=transform.name,
            description=transform.description,
            input_value=input_val,
            output_value=output_val,
        )
    ]


def _toposort_sources(
    sources: list[tuple[str, Any]],
) -> list[tuple[str, Any]]:
    """Sort sources by dependency (linked_to) order.

    Sources without dependencies come first. Sources that depend on others
    come after their dependencies.
    """
    if len(sources) <= 1:
        return sources

    id_to_entry = {id(src): (name, src) for name, src in sources}
    in_degree: dict[int, int] = {id(src): 0 for _, src in sources}
    dependents: dict[int, list[int]] = {id(src): [] for _, src in sources}

    for _, src in sources:
        linked = getattr(src, "_linked_to", None)
        if linked is not None and id(linked) in id_to_entry:
            in_degree[id(src)] += 1
            dependents[id(linked)].append(id(src))

    # Kahn's algorithm
    queue = [sid for sid, deg in in_degree.items() if deg == 0]
    result: list[tuple[str, Any]] = []

    while queue:
        sid = queue.pop(0)
        result.append(id_to_entry[sid])
        for dep_id in dependents[sid]:
            in_degree[dep_id] -= 1
            if in_degree[dep_id] == 0:
                queue.append(dep_id)

    if len(result) != len(sources):
        raise TypeError("Circular source dependency detected")

    return result


def _merge_linked_rows(
    all_rows: list[dict[str, Any]],
    new_rows: list[dict[str, Any]],
    bound_fields: list[tuple[str, Any]],
    _source: Any,
    pk_field_attr: str,
) -> None:
    """Merge data from a linked source into existing rows.

    For each bound field, copies the value from new_rows into all_rows
    by matching on position (if same length) or PK.
    """
    field_attrs = [attr for attr, _ in bound_fields]

    if len(new_rows) == len(all_rows):
        # Same-length - positional merge
        for i, row in enumerate(all_rows):
            for attr in field_attrs:
                if attr in new_rows[i]:
                    row[attr] = new_rows[i][attr]
    else:
        # Build index on new rows by PK
        pk_key = f"_pk_{pk_field_attr}"
        new_by_pk: dict[str, dict] = {}
        for nr in new_rows:
            pk = nr.get(pk_key, nr.get(pk_field_attr, ""))
            if pk:
                new_by_pk[pk] = nr

        for row in all_rows:
            pk = row.get(pk_key, row.get(pk_field_attr, ""))
            matched = new_by_pk.get(pk)
            if matched:
                for attr in field_attrs:
                    if attr in matched:
                        row[attr] = matched[attr]


def _fetch_all_sources(
    cls: type,
    cache: Any,
    sorted_sources: list[tuple[str, Any]],
    pk_field_attr: str,
    refresh: bool,
) -> list[dict[str, Any]]:
    """Fetch data from all sources and merge into a single row list."""
    all_rows: list[dict[str, Any]] = []
    for source_attr, source in sorted_sources:
        src_id = id(source)
        bound_fields = cls._fields_by_source.get(src_id, [])
        if not bound_fields:
            continue
        log_info(f"Source '{source_attr}': fetching {len(bound_fields)} fields")
        if hasattr(source, "_linked_to") and source._linked_to is not None and all_rows:
            rows = source.fetch(bound_fields, cache, refresh)
            _merge_linked_rows(all_rows, rows, bound_fields, source, pk_field_attr)
        else:
            rows = source.fetch(bound_fields, cache, refresh)
            if not all_rows:
                all_rows = rows
            else:
                _merge_linked_rows(all_rows, rows, bound_fields, source, pk_field_attr)
    return all_rows


def _init_provenance(
    cls: type,
    all_rows: list[dict[str, Any]],
    sorted_sources: list[tuple[str, Any]],
) -> list[dict[str, Any]]:
    """Build initial provenance records for all source-backed fields.

    Each source that implements build_provenance_records handles its own
    provenance construction. A generic fallback is used for sources that do
    not implement the protocol.
    """
    from datetime import UTC
    from datetime import datetime as _dt

    from ankitron.provenance import ProvenanceRecord

    all_provenance: list[dict[str, Any]] = [{} for _ in all_rows]

    for source_attr, source in sorted_sources:
        src_id = id(source)
        bound_fields = cls._fields_by_source.get(src_id, [])
        if not bound_fields:
            continue

        if hasattr(source, "build_provenance_records"):
            source_prov = source.build_provenance_records(all_rows, bound_fields, source_attr)
            for row_idx, row_prov in enumerate(source_prov):
                if row_idx < len(all_provenance):
                    all_provenance[row_idx].update(row_prov)
        else:
            # Generic fallback for sources without the provenance protocol
            cache_info = getattr(source, "_last_cache_info", None)
            cached = cache_info.get("cached", False) if cache_info else False
            fetched_at = _dt.now(UTC)
            for row_idx, row in enumerate(all_rows):
                for attr_name, fld in bound_fields:
                    if fld.is_derived or fld.is_computed or fld.is_cascade:
                        continue
                    raw_val = row.get(attr_name)
                    all_provenance[row_idx][attr_name] = ProvenanceRecord(
                        source_type=type(source).__name__,
                        source_name=source_attr,
                        source_key=fld._source_key,
                        raw_value=raw_val,
                        raw_type=type(raw_val).__name__,
                        fetched_at=fetched_at,
                        cached=cached,
                    )

    return all_provenance


def _check_field_rules(cls: type, all_rows: list[dict[str, Any]]) -> None:
    """Enforce FieldRule constraints -- REQUIRED raises, EXPECTED warns."""
    for attr_name, fld in cls._all_fields:
        if fld.is_derived or fld.is_computed:
            continue
        if fld.rule == FieldRule.OPTIONAL:
            continue
        missing_pks = []
        for row in all_rows:
            val = row.get(attr_name)
            if val is None or (isinstance(val, str) and val.strip() == ""):
                pk_val = row.get(f"_pk_{cls._pk_field_attr}", row.get(cls._pk_field_attr, "?"))
                missing_pks.append(str(pk_val))
        if missing_pks:
            msg = (
                f"Field '{attr_name}' ({fld.rule.value}): {len(missing_pks)} row(s) missing values"
            )
            if fld.rule == FieldRule.REQUIRED:
                raise RuntimeError(f"{cls.__name__}: {msg}. Aborting.")
            if fld.rule == FieldRule.EXPECTED:
                log_warn(f"{cls.__name__}: {msg}")


def _apply_defaults(cls: type, all_rows: list[dict[str, Any]]) -> None:
    """Fill in declared default values for empty fields."""
    for attr_name, fld in cls._all_fields:
        if fld.default is None or fld.is_derived or fld.is_computed:
            continue
        for row in all_rows:
            val = row.get(attr_name)
            if val is None or (isinstance(val, str) and val.strip() == ""):
                row[attr_name] = (
                    str(fld.default) if not isinstance(fld.default, str) else fld.default
                )


def _apply_overrides(
    cls: type,
    all_rows: list[dict[str, Any]],
    all_provenance: list[dict[str, Any]],
    prov_enabled: bool,
) -> None:
    """Apply per-PK overrides from cls._deck_overrides."""
    if not cls._deck_overrides:
        return
    pk_attr = cls._pk_field_attr
    overrides_applied = 0
    for row_idx, row in enumerate(all_rows):
        pk_val = row.get(f"_pk_{pk_attr}", row.get(pk_attr, ""))
        if pk_val in cls._deck_overrides:
            for override_field, override_val in cls._deck_overrides[pk_val].items():
                if prov_enabled and row_idx < len(all_provenance):
                    prov_rec = all_provenance[row_idx].get(override_field)
                    if prov_rec:
                        prov_rec.overridden = True
                        prov_rec.original_value = row.get(override_field)
                row[override_field] = (
                    str(override_val) if not isinstance(override_val, str) else override_val
                )
                overrides_applied += 1
    if overrides_applied:
        log_info(f"  {overrides_applied} override(s) applied")


def _apply_cascade(
    cls: type,
    all_rows: list[dict[str, Any]],
    all_provenance: list[dict[str, Any]],
    prov_enabled: bool,
) -> None:
    """Resolve cascade fields, picking the first non-empty value."""
    cascade_fields = [(n, f) for n, f in cls._all_fields if f.is_cascade]
    if not cascade_fields:
        return
    from ankitron.provenance import ProvenanceRecord

    field_id_to_attr = {id(fld): name for name, fld in cls._all_fields}
    for attr_name, fld in cascade_fields:
        source_attrs = [field_id_to_attr[id(src)] for src in fld._cascade_sources]
        for row_idx, row in enumerate(all_rows):
            chosen_attr: str | None = None
            for src_attr in source_attrs:
                val = row.get(src_attr)
                if val is not None and (not isinstance(val, str) or val.strip()):
                    row[attr_name] = val
                    chosen_attr = src_attr
                    break
            else:
                row[attr_name] = ""
            if prov_enabled and row_idx < len(all_provenance):
                all_provenance[row_idx][attr_name] = ProvenanceRecord(
                    source_type="cascade",
                    source_name="",
                    raw_value=row[attr_name],
                    raw_type=type(row[attr_name]).__name__,
                    derived_from=chosen_attr,
                )


def _apply_derivations(
    cls: type,
    all_rows: list[dict[str, Any]],
    all_provenance: list[dict[str, Any]],
    prov_enabled: bool,
) -> None:
    """Apply derived and computed fields in topological order."""
    if not cls._derived_order:
        return
    from ankitron.provenance import ProvenanceRecord, TransformStep

    field_id_to_attr = {id(fld): name for name, fld in cls._all_fields}
    transforms_applied = []

    for attr_name, fld in cls._derived_order:
        if fld.is_computed:
            input_attrs = [field_id_to_attr[id(inp)] for inp in fld._computed_inputs]
            transforms_applied.append(f"{attr_name} ← computed({', '.join(input_attrs)})")

            for row_idx, row in enumerate(all_rows):
                input_vals = [_coerce_numeric(row.get(ia, "")) for ia in input_attrs]
                try:
                    val = fld._computed_fn(*input_vals)
                except Exception as exc:
                    pk_val = row.get(f"_pk_{cls._pk_field_attr}", row.get(cls._pk_field_attr, "?"))
                    raise RuntimeError(
                        f"{cls.__name__}: computed field '{attr_name}' failed "
                        f"on row '{pk_val}': {type(exc).__name__}: {exc}."
                    ) from exc
                row[attr_name] = str(val) if val is not None else ""
                if prov_enabled and row_idx < len(all_provenance):
                    fn_name = getattr(fld._computed_fn, "__name__", "computed_fn")
                    raw_inputs = {ia: _coerce_numeric(row.get(ia, "")) for ia in input_attrs}
                    all_provenance[row_idx][attr_name] = ProvenanceRecord(
                        source_type="computed",
                        source_name="",
                        raw_value=raw_inputs,
                        raw_type="dict",
                        transformed=True,
                        transform_chain=[
                            TransformStep(
                                name=fn_name,
                                description=f"computed({', '.join(input_attrs)})",
                                input_value=raw_inputs,
                                output_value=val,
                            )
                        ],
                        computed_from=input_attrs,
                        formatted_value=row[attr_name],
                    )

        elif fld.is_derived:
            parent_attr = field_id_to_attr[id(fld._parent)]
            transforms_applied.append(f"{attr_name} ← {parent_attr}")

            if isinstance(fld._transform, Transform):
                if fld._transform.is_dataset_aware:
                    parent_vals = [row.get(parent_attr, "") for row in all_rows]
                    converted = [_coerce_numeric(v) if v != "" else None for v in parent_vals]
                    results = apply_transform_chain(fld._transform, converted)
                    for row_idx, (row, conv_val, result_val) in enumerate(
                        zip(all_rows, converted, results, strict=False)
                    ):
                        row[attr_name] = str(result_val) if result_val is not None else ""
                        if prov_enabled and row_idx < len(all_provenance):
                            steps = _build_transform_steps_for_prov(
                                fld._transform, conv_val, result_val
                            )
                            all_provenance[row_idx][attr_name] = ProvenanceRecord(
                                source_type="derived",
                                source_name="",
                                derived_from=parent_attr,
                                raw_value=conv_val,
                                raw_type=type(conv_val).__name__,
                                transformed=True,
                                transform_chain=steps,
                                formatted_value=row[attr_name],
                            )
                else:
                    for row_idx, row in enumerate(all_rows):
                        parent_val = row.get(parent_attr, "")
                        coerced_in = _coerce_numeric(parent_val)
                        result_val = coerced_in
                        try:
                            result_val = fld._transform.apply(coerced_in)
                        except Exception as exc:
                            pk_val = row.get(
                                f"_pk_{cls._pk_field_attr}",
                                row.get(cls._pk_field_attr, "?"),
                            )
                            raise RuntimeError(
                                f"{cls.__name__}: transform failed for field '{attr_name}' "
                                f"on row '{pk_val}': {type(exc).__name__}: {exc}. "
                                f"The source value was {parent_val!r}."
                            ) from exc
                        row[attr_name] = str(result_val) if result_val is not None else ""
                        if prov_enabled and row_idx < len(all_provenance):
                            steps = _build_transform_steps_for_prov(
                                fld._transform, coerced_in, result_val
                            )
                            all_provenance[row_idx][attr_name] = ProvenanceRecord(
                                source_type="derived",
                                source_name="",
                                derived_from=parent_attr,
                                raw_value=coerced_in,
                                raw_type=type(coerced_in).__name__,
                                transformed=True,
                                transform_chain=steps,
                                formatted_value=row[attr_name],
                            )
            else:
                # Legacy callable transform (or bare copy with no transform)
                for row_idx, row in enumerate(all_rows):
                    parent_val = row.get(parent_attr, "")
                    coerced_in = _coerce_numeric(parent_val)
                    result_val = coerced_in
                    if fld._transform is not None:
                        try:
                            result_val = fld._transform(coerced_in)
                        except Exception as exc:
                            pk_val = row.get(
                                f"_pk_{cls._pk_field_attr}",
                                row.get(cls._pk_field_attr, "?"),
                            )
                            raise RuntimeError(
                                f"{cls.__name__}: transform failed for field '{attr_name}' "
                                f"on row '{pk_val}': {type(exc).__name__}: {exc}. "
                                f"The source value was {parent_val!r}."
                            ) from exc
                    row[attr_name] = str(result_val) if result_val is not None else ""
                    if prov_enabled and row_idx < len(all_provenance):
                        fn_name = (
                            getattr(fld._transform, "__name__", "custom")
                            if fld._transform is not None
                            else "copy"
                        )
                        chain = (
                            [
                                TransformStep(
                                    name=fn_name,
                                    description=f"{parent_attr} → {attr_name}",
                                    input_value=coerced_in,
                                    output_value=result_val,
                                )
                            ]
                            if fld._transform is not None
                            else []
                        )
                        all_provenance[row_idx][attr_name] = ProvenanceRecord(
                            source_type="derived",
                            source_name="",
                            derived_from=parent_attr,
                            raw_value=coerced_in,
                            raw_type=type(coerced_in).__name__,
                            transformed=fld._transform is not None,
                            transform_chain=chain,
                            formatted_value=row[attr_name],
                        )

        # Apply fmt for this derived/computed field
        if fld.fmt:
            for row_idx, row in enumerate(all_rows):
                val = row.get(attr_name, "")
                if val:
                    numeric = _coerce_numeric(val)
                    if isinstance(numeric, (int, float)):
                        with contextlib.suppress(ValueError, TypeError):
                            formatted = fld.fmt.format(numeric)
                            row[attr_name] = formatted
                            if prov_enabled and row_idx < len(all_provenance):
                                rec = all_provenance[row_idx].get(attr_name)
                                if rec:
                                    rec.fmt = fld.fmt
                                    rec.formatted_value = formatted

    log_success(
        f"{len(transforms_applied)} "
        f"transform{'s' if len(transforms_applied) != 1 else ''} "
        f"applied: {', '.join(transforms_applied)}"
    )


def _apply_source_formatting(
    cls: type,
    all_rows: list[dict[str, Any]],
    all_provenance: list[dict[str, Any]],
    prov_enabled: bool,
) -> None:
    """Apply fmt formatting to non-derived source fields."""
    for attr_name, fld in cls._all_fields:
        if fld.is_derived or fld.is_computed or not fld.fmt:
            continue
        for row_idx, row in enumerate(all_rows):
            val = row.get(attr_name, "")
            if val:
                numeric = _coerce_numeric(val)
                if isinstance(numeric, (int, float)):
                    with contextlib.suppress(ValueError, TypeError):
                        formatted = fld.fmt.format(numeric)
                        row[attr_name] = formatted
                        if prov_enabled and row_idx < len(all_provenance):
                            prov_rec = all_provenance[row_idx].get(attr_name)
                            if prov_rec:
                                prov_rec.fmt = fld.fmt
                                prov_rec.formatted_value = formatted


def _apply_provenance_backfill(
    cls: type,
    all_rows: list[dict[str, Any]],
    all_provenance: list[dict[str, Any]],
) -> None:
    """Safety-net: add a minimal ProvenanceRecord for any field not yet tracked.

    In normal operation all fields are populated inline by the steps above;
    this only fires for edge cases such as a new field type added without
    provenance wiring.
    """
    from ankitron.provenance import ProvenanceRecord

    field_id_to_attr = {id(fld): name for name, fld in cls._all_fields}
    for row_idx in range(len(all_rows)):
        if row_idx >= len(all_provenance):
            break
        prov_row = all_provenance[row_idx]
        for attr_name, fld in cls._all_fields:
            if attr_name in prov_row:
                continue
            rec = ProvenanceRecord(
                source_type=(
                    "derived"
                    if fld.is_derived
                    else "computed"
                    if fld.is_computed
                    else "cascade"
                    if fld.is_cascade
                    else "unknown"
                ),
                source_name="",
                raw_value=all_rows[row_idx].get(attr_name),
                raw_type=type(all_rows[row_idx].get(attr_name)).__name__,
            )
            if fld.is_derived and fld._parent:
                rec.derived_from = field_id_to_attr.get(id(fld._parent))
            if fld.is_computed and fld._computed_inputs:
                rec.computed_from = [
                    field_id_to_attr.get(id(inp), "?") for inp in fld._computed_inputs
                ]
            prov_row[attr_name] = rec


def _run_validators(
    cls: type,
    all_rows: list[dict[str, Any]],
    skip_validation: bool,
) -> None:
    """Run declared validators, raising on ERROR severity failures."""
    if not cls._deck_validators or skip_validation:
        return
    from ankitron.validation import run_validators

    results = run_validators(cls._deck_validators, all_rows)
    for result in results:
        if result.passed:
            log_success(f"Validator '{result.name}': passed")
        else:
            msg = f"Validator '{result.name}': FAILED -- {'; '.join(result.messages[:3])}"
            if result.severity == Severity.ERROR:
                raise RuntimeError(f"{cls.__name__}: {msg}")
            log_warn(msg)
