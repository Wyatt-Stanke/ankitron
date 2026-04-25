"""
Provenance — data lineage tracking for every field value.

Tracks origin, transforms, verification, overrides, and AI generation
metadata for each cell (one field x one row). Provenance can be
embedded in exported Anki cards as a hidden JSON field with a
rendered HTML/CSS/JS panel on card backs.
"""

from __future__ import annotations

import enum
import json
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any


class ProvenancePosition(enum.Enum):
    """Where provenance is rendered on card backs."""

    FOOTER = "footer"
    HIDDEN = "hidden"
    NONE = "none"


class ProvenanceStyle(enum.Enum):
    """How provenance is rendered."""

    COLLAPSED = "collapsed"
    INLINE = "inline"
    DETAILED = "detailed"


def is_provenance_enabled(cls: type) -> bool:
    """Return True if provenance is enabled and configured to render on cards."""
    prov_config: ProvenanceConfig | None = getattr(cls, "provenance", None)  # type: ignore[assignment]
    return (
        prov_config is not None
        and prov_config.enabled
        and prov_config.position != ProvenancePosition.NONE
    )


@dataclass
class ProvenanceConfig:
    """Master configuration for provenance tracking and rendering."""

    enabled: bool = True
    position: ProvenancePosition = ProvenancePosition.FOOTER
    style: ProvenanceStyle = ProvenanceStyle.COLLAPSED
    allow_flags: bool = True
    show_ai_badge: bool = True
    show_stale_indicator: bool = True


@dataclass
class TransformStep:
    """One step in a transform chain, with input/output for debugging."""

    name: str
    description: str
    input_value: Any
    output_value: Any


@dataclass
class ProvenanceRecord:
    """Full provenance for one field value in one row."""

    # Origin
    source_type: str = ""
    source_name: str = ""
    source_key: str | None = None
    source_url: str | None = None
    source_entity_id: str | None = None

    # Raw value
    raw_value: Any = None
    raw_type: str = "NoneType"

    # Transform chain
    transformed: bool = False
    transform_chain: list[TransformStep] = dc_field(default_factory=list)

    # Formatting
    fmt: str | None = None
    formatted_value: str | None = None

    # Derivation
    derived_from: str | None = None
    computed_from: list[str] | None = None

    # Verification
    verification: Any | None = None

    # Override
    overridden: bool = False
    original_value: Any | None = None

    # AI generation
    ai_generated: bool = False
    ai_model: str | None = None
    ai_prompt_template: str | None = None
    ai_prompt_resolved: str | None = None
    ai_reviewed: bool = False

    # Timing
    fetched_at: datetime | None = None
    cached: bool = False
    cache_expires_at: datetime | None = None

    # Flagging
    flagged: bool = False
    flagged_at: datetime | None = None
    flag_note: str | None = None


def provenance_to_json(
    provenance: dict[str, ProvenanceRecord],
    deck_name: str,
    pk: str,
    pk_display: str,
    visible_fields: list[str] | None = None,
) -> str:
    """Serialize provenance records to compact JSON for embedding in Anki notes.

    Format v1 (compact):
      Envelope: {"v":1, "src":[type,name], "f":{...}}

      Per-field values (union):
        "key"              — source_key, dominant src, flags=0  (most common)
        ["key",flags]      — dominant src, non-zero flags
        ["key",flags,extra]— dominant src, extra={u?,m?,fn?}
        ["key",flags,src]  — non-dominant src=[type,name,?url]
        ["key",flags,src,extra] — non-dominant src + extra

      Flags bitmask: 1=cached 2=overridden 4=ai 8=ai_reviewed 16=verified 32=flagged
    """
    from collections import Counter

    records = provenance
    if visible_fields is not None:
        records = {k: v for k, v in provenance.items() if k in visible_fields}

    if not records:
        return ""

    # Hoist the most common (source_type, source_name) pair to the envelope
    source_pairs = [
        (rec.source_type or "", rec.source_name or "")
        for rec in records.values()
        if rec.source_type
    ]
    counter: Counter = Counter(source_pairs)
    dominant = counter.most_common(1)[0][0] if counter else ("", "")

    fields_data: dict[str, Any] = {}
    for fname, rec in records.items():
        # Bitmask for boolean flags — omit entirely when 0
        flags = 0
        if rec.cached:       flags |= 1
        if rec.overridden:   flags |= 2
        if rec.ai_generated: flags |= 4
        if rec.ai_reviewed:  flags |= 8
        if rec.verification: flags |= 16
        if rec.flagged:      flags |= 32

        key = rec.source_key or ""
        is_dominant = (rec.source_type or "", rec.source_name or "") == dominant

        # Rare extras only present when needed
        extra: dict[str, Any] = {}
        if rec.source_url:
            extra["u"] = rec.source_url
        if rec.ai_generated and rec.ai_model:
            extra["m"] = rec.ai_model
        if rec.flagged and rec.flag_note:
            extra["fn"] = rec.flag_note

        if is_dominant:
            if flags == 0 and not extra:
                fields_data[fname] = key          # simplest: bare string
            elif extra:
                fields_data[fname] = [key, flags, extra]
            else:
                fields_data[fname] = [key, flags]
        else:
            src_override: list[Any] = [rec.source_type or "", rec.source_name or ""]
            if rec.source_url:
                src_override.append(rec.source_url)
                extra.pop("u", None)  # already encoded in src_override
            if extra:
                fields_data[fname] = [key, flags, src_override, extra]
            else:
                fields_data[fname] = [key, flags, src_override]

    envelope: dict[str, Any] = {"v": 1}
    if dominant[0] or dominant[1]:
        envelope["src"] = list(dominant)
    envelope["f"] = fields_data
    return json.dumps(envelope, default=str, separators=(",", ":"))


# ── HTML/CSS/JS rendering ──


_PROVENANCE_CSS = (
    ".ankitron-prov{font-family:system-ui,-apple-system,sans-serif;"
    "font-size:12px;margin-top:16px;border-top:1px solid var(--border,#eee);"
    "padding-top:8px}\n"
    ".ankitron-prov-toggle{background:var(--canvas-elevated,#f5f5f5);"
    "border:1px solid var(--border,#ddd);border-radius:4px;padding:4px 10px;"
    "cursor:pointer;font-size:12px;color:var(--fg,#666)}\n"
    ".ankitron-prov-toggle:hover{background:var(--canvas-elevated,#e8e8e8)}\n"
    ".ankitron-prov-detail{margin-top:8px;padding:8px;"
    "background:var(--canvas-elevated,#fafafa);"
    "border:1px solid var(--border,#eee);border-radius:4px;color:var(--fg,#333)}\n"
    ".ankitron-prov-field{margin-bottom:6px;padding:4px 0;"
    "border-bottom:1px solid var(--border,#f0f0f0)}\n"
    ".ankitron-prov-field:last-child{border-bottom:none;margin-bottom:0}\n"
    ".ankitron-prov-label{font-weight:600;color:var(--fg,#222)}\n"
    ".ankitron-prov-source{color:var(--fg-subtle,#666);font-size:11px}\n"
    ".ankitron-prov-ai{display:inline-block;background:#e8d5f5;color:#6b21a8;"
    "padding:1px 5px;border-radius:3px;font-size:10px;font-weight:600;"
    "margin-left:4px}\n"
    ".ankitron-prov-override{color:#b45309;font-size:11px;font-style:italic}\n"
    ".ankitron-prov-verified{color:#16a34a;font-size:11px}\n"
    ".ankitron-prov-warn{color:#d97706;font-size:11px}\n"
    ".ankitron-prov-flag-btn{background:none;border:none;cursor:pointer;"
    "font-size:14px;padding:0 2px}"
)

_PROVENANCE_JS = (
    "(function(){"
    "try{"
    "var d=JSON.parse(document.getElementById('ankitron-prov-data').textContent);"
    "var c=document.getElementById('ankitron-prov-detail');"
    "var af=document.querySelector('.ankitron-prov');"
    "var fields=af&&af.dataset.fields?af.dataset.fields.split(','):null;"
    "var f=d.f||{};"
    "var ds=d.src||['',''];"
    "var html='';"
    "for(var k in f){"
    "if(fields&&fields.indexOf(k)<0)continue;"
    "var v=f[k],key,fl=0,src=ds,ex={};"
    "if(v===null){key='';}"
    "else if(typeof v==='string'){key=v;}"
    "else{key=v[0];fl=v[1]||0;"
    "if(v[2]){if(Array.isArray(v[2])){src=v[2];ex=v[3]||{};}else{ex=v[2];}}}"
    "var sn=src[1]||src[0]||'';"
    "var su=(src&&src[2])||ex.u||null;"
    "html+='<div class=\"ankitron-prov-field\">';"
    "html+='<span class=\"ankitron-prov-label\">'+k+'</span> ';"
    "if(fl&4)html+='<span class=\"ankitron-prov-ai\">AI</span>';"
    "if(sn){html+='<br><span class=\"ankitron-prov-source\">';"
    "if(su)html+='<a href=\"'+su+'\" style=\"color:inherit\">'+sn+'</a>';"
    "else html+=sn;"
    "if(key)html+=' \u2192 '+key;"
    "html+='</span>';}"
    "if(fl&2)html+='<br><span class=\"ankitron-prov-override\">overridden</span>';"
    "if(fl&16)html+='<br><span class=\"ankitron-prov-verified\">\u2713 verified</span>';"
    "if(fl&32)html+='<br><span class=\"ankitron-prov-warn\">\u26a0 flagged'+(ex.fn?' \u2014 '+ex.fn:'')+'</span>';"
    "html+='</div>';}"
    "c.innerHTML=html;"
    "}catch(e){}"
    "})();"
)


def render_provenance_html(config: ProvenanceConfig, card_fields: list[str] | None = None) -> str:
    """Generate the HTML/CSS/JS to inject into card back templates.

    Args:
        config: Provenance configuration.
        card_fields: List of field names this card template references.
    """
    if not config.enabled or config.position == ProvenancePosition.NONE:
        return ""

    fields_attr = ""
    if card_fields:
        fields_attr = f' data-fields="{",".join(card_fields)}"'

    if config.style == ProvenanceStyle.COLLAPSED:
        toggle = (
            '<button class="ankitron-prov-toggle" onclick="'
            "var el=document.getElementById('ankitron-prov-detail');"
            "el.style.display=el.style.display==='none'?'block':'none';"
            '">📋 Sources</button>'
        )
        detail_style = ' style="display:none;"'
    elif config.style == ProvenanceStyle.INLINE:
        toggle = '<div class="ankitron-prov-toggle" style="font-size:11px;border:none;padding:0;">📋 Provenance</div>'
        detail_style = ""
    else:  # DETAILED
        toggle = '<div class="ankitron-prov-toggle" style="font-size:11px;border:none;padding:0;">📋 Full Provenance</div>'
        detail_style = ""

    html = f"""<style>{_PROVENANCE_CSS}</style>
<div class="ankitron-prov"{fields_attr}>
{toggle}
<div class="ankitron-prov-detail" id="ankitron-prov-detail"{detail_style}></div>
</div>
<script type="application/json" id="ankitron-prov-data">{{{{_ankitron_provenance}}}}</script>
<script>{_PROVENANCE_JS}</script>"""

    return html  # noqa: RET504
