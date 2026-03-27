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
from datetime import UTC, datetime
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
    """Serialize provenance records to compressed JSON for embedding in Anki notes.

    Only includes fields listed in visible_fields (if provided).
    Uses short keys for compression.
    """
    fields_data: dict[str, Any] = {}
    records = provenance
    if visible_fields is not None:
        records = {k: v for k, v in provenance.items() if k in visible_fields}

    for fname, rec in records.items():
        fd: dict[str, Any] = {
            "value": rec.formatted_value or str(rec.raw_value) if rec.raw_value is not None else "",
            "source": rec.source_type,
            "source_name": rec.source_name,
        }
        if rec.source_key:
            fd["source_key"] = rec.source_key
        if rec.source_url:
            fd["source_url"] = rec.source_url

        # Raw value only if different from formatted
        raw_str = str(rec.raw_value) if rec.raw_value is not None else ""
        if raw_str and raw_str != fd["value"]:
            fd["raw_value"] = rec.raw_value

        # Transform chain
        if rec.transform_chain:
            fd["transforms"] = [
                {"name": s.name, "desc": s.description, "in": s.input_value, "out": s.output_value}
                for s in rec.transform_chain
            ]

        if rec.fmt:
            fd["fmt"] = rec.fmt
        if rec.fetched_at:
            fd["fetched_at"] = rec.fetched_at.isoformat()
        fd["cached"] = rec.cached
        if rec.verification:
            fd["verified"] = True
        fd["overridden"] = rec.overridden
        fd["ai"] = rec.ai_generated
        if rec.ai_generated:
            if rec.ai_model:
                fd["ai_model"] = rec.ai_model
            fd["ai_reviewed"] = rec.ai_reviewed
        fd["flagged"] = rec.flagged
        if rec.flagged and rec.flag_note:
            fd["flag_note"] = rec.flag_note

        fields_data[fname] = fd

    envelope = {
        "version": 1,
        "deck": deck_name,
        "built_at": datetime.now(UTC).isoformat(),
        "pk": pk,
        "pk_display": pk_display,
        "fields": fields_data,
    }
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

_PROVENANCE_JS = """\
(function(){
try{
var d=JSON.parse(document.getElementById('ankitron-prov-data').textContent);
var c=document.getElementById('ankitron-prov-detail');
var af=document.querySelector('.ankitron-prov');
var fields=af&&af.dataset.fields?af.dataset.fields.split(','):null;
var f=d.fields||{};
var html='';
for(var k in f){
if(fields&&fields.indexOf(k)<0)continue;
var v=f[k];
html+='<div class="ankitron-prov-field">';
html+='<span class="ankitron-prov-label">'+k+'</span> ';
if(v.ai)html+='<span class="ankitron-prov-ai">AI</span>';
if(v.source_url)html+='<br><span class="ankitron-prov-source"><a href="'+v.source_url+'" style="color:inherit">'+v.source_name+'</a>';
else html+='<br><span class="ankitron-prov-source">'+v.source_name;
if(v.source_key)html+=' → '+v.source_key;
html+='</span>';
if(v.overridden)html+='<br><span class="ankitron-prov-override">overridden</span>';
if(v.verified)html+='<br><span class="ankitron-prov-verified">✓ verified</span>';
if(v.flagged)html+='<br><span class="ankitron-prov-warn">⚠ flagged'+(v.flag_note?' — '+v.flag_note:'')+'</span>';
html+='</div>';
}
c.innerHTML=html;
}catch(e){}
})();"""


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
