"""Assemble the dashboard into one self-contained HTML document. Pure.

SELF-CONTAINED IS A REQUIREMENT, NOT A PREFERENCE. The AWS account is wiped every
seven days, so "show me it worked" has to be answerable from a file on a laptop
with no network and no running stack. No CDN, no bundle step, no external font.

DARK MODE IS SELECTED, NOT FLIPPED. The dark series colours are their own
validated steps against the dark surface, not the light ones inverted. Both sets
cleared the same gates. They are declared under a media query for the OS setting
AND under a data-theme scope for an explicit toggle, with the toggle winning both
directions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from html import escape

from awsnative.dashboard.charts import SERIES_DARK, SERIES_LIGHT

_STYLE = f"""
:root {{ color-scheme: light dark; }}
.viz-root {{
  --surface-1: #fcfcfb; --surface-2: #f4f3f0; --border: #e2e1dc;
  --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #6f6e6a;
  --series-1: {SERIES_LIGHT[0]}; --series-2: {SERIES_LIGHT[1]}; --series-3: {SERIES_LIGHT[2]};
  --grid: #e8e7e2;
}}
@media (prefers-color-scheme: dark) {{
  :root:where(:not([data-theme="light"])) .viz-root {{
    --surface-1: #1a1a19; --surface-2: #232322; --border: #383835;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #9a9990;
    --series-1: {SERIES_DARK[0]}; --series-2: {SERIES_DARK[1]}; --series-3: {SERIES_DARK[2]};
    --grid: #2e2e2c;
  }}
}}
:root[data-theme="dark"] .viz-root {{
  --surface-1: #1a1a19; --surface-2: #232322; --border: #383835;
  --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #9a9990;
  --series-1: {SERIES_DARK[0]}; --series-2: {SERIES_DARK[1]}; --series-3: {SERIES_DARK[2]};
  --grid: #2e2e2c;
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--surface-2); }}
.viz-root {{
  background: var(--surface-2); color: var(--text-primary); min-height: 100vh;
  font: 14px/1.5 ui-sans-serif, -apple-system, "Segoe UI", Roboto, sans-serif;
  padding: 28px clamp(16px, 4vw, 48px) 64px;
}}
header.viz-head {{ display:flex; align-items:baseline; gap:16px;
                    flex-wrap:wrap; margin-bottom:4px; }}
h1 {{ font-size: 20px; margin: 0; letter-spacing: -0.01em; }}
.viz-meta {{ color: var(--text-muted); font-size: 12px; font-variant-numeric: tabular-nums; }}
.viz-lede {{ color: var(--text-secondary); max-width: 68ch; margin: 8px 0 24px; }}
h2 {{ font-size: 13px; text-transform: uppercase; letter-spacing: .07em;
     color: var(--text-secondary); margin: 32px 0 12px; font-weight: 600; }}
.viz-tiles {{ display:grid; gap:12px;
               grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); }}
.viz-tile {{ background: var(--surface-1); border:1px solid var(--border);
              border-radius:10px; padding:14px 16px; }}
.viz-tile-label {{ color: var(--text-secondary); font-size:12px; }}
.viz-tile-value {{ font-size:26px; font-weight:600; letter-spacing:-0.02em;
                   font-variant-numeric: tabular-nums; margin:2px 0 6px; }}
.viz-tile-state {{ font-size:12px; color: var(--text-secondary);
                    display:flex; align-items:center; gap:6px; }}
.viz-tile-state strong {{ color: var(--text-primary); }}
.viz-dot {{ width:9px; height:9px; border-radius:50%; flex:none; }}
.viz-flow {{ display:flex; flex-direction:column; gap:16px; }}
.viz-grid-2 {{ display:grid; gap:16px;
                grid-template-columns:repeat(auto-fit,minmax(340px,1fr)); }}
.viz-panel {{ background: var(--surface-1); border:1px solid var(--border);
              border-radius:10px; padding:14px 16px 8px; margin:0; }}
.viz-panel svg {{ width:100%; height:auto; display:block; overflow:visible; }}
figcaption {{ display:flex; gap:10px; align-items:baseline;
               flex-wrap:wrap; margin-bottom:6px; }}
.viz-title {{ font-weight:600; }}
.viz-sub {{ color: var(--text-muted); font-size:12px; }}
.viz-legend {{ display:flex; gap:14px; flex-wrap:wrap; margin:2px 0 8px; font-size:12px;
               color: var(--text-secondary); }}
.viz-key {{ display:inline-flex; align-items:center; gap:6px; }}
.viz-key i {{ width:10px; height:10px; border-radius:3px; display:inline-block; }}
.viz-grid {{ stroke: var(--grid); stroke-width:1; }}
.viz-zero {{ stroke: var(--text-muted); stroke-width:1; stroke-dasharray:3 3; }}
.viz-tick {{ fill: var(--text-muted); font-size:10px; font-variant-numeric: tabular-nums; }}
.viz-direct {{ fill: var(--text-primary); font-size:11px; font-weight:600;
               font-variant-numeric: tabular-nums; }}
.viz-end {{ stroke: var(--surface-1); stroke-width:2; }}
.viz-hit {{ cursor: crosshair; }}
.viz-hit:hover circle[r="10"] {{
  fill: color-mix(in oklab, var(--text-primary) 12%, transparent); }}
.viz-nodata {{ display:grid; place-items:center; color: var(--text-muted); font-size:13px;
               border:1px dashed var(--border); border-radius:8px; }}
.viz-table {{ margin-top:10px; font-size:12px; }}
.viz-table summary {{ cursor:pointer; color: var(--text-secondary); }}
.viz-table table {{ border-collapse:collapse; margin-top:8px; width:100%; }}
.viz-table th, .viz-table td {{ border-bottom:1px solid var(--border);
                                 padding:4px 8px;
                                text-align:right; font-variant-numeric: tabular-nums; }}
.viz-table th:first-child, .viz-table td:first-child {{ text-align:left; }}
footer.viz-foot {{ margin-top:40px; color: var(--text-muted); font-size:12px;
                   border-top:1px solid var(--border); padding-top:12px; max-width:80ch; }}
@media print {{ .viz-panel {{ break-inside: avoid; }} }}
"""


@dataclass(frozen=True, slots=True)
class Section:
    """A headed group of blocks.

    `layout` is not cosmetic. "tiles" packs fixed-width stat cards; "flow" stacks
    full-width figures. Mixing the two -- a table-view disclosure dropped into a
    tile grid -- makes the disclosure occupy a card slot and the row read as a
    broken card, which is exactly what the first render of this page did.
    """

    heading: str
    blocks: tuple[str, ...] = field(default_factory=tuple)
    layout: str = "flow"


def build_page(
    *,
    title: str,
    generated_at: str,
    database: str,
    lede: str,
    sections: tuple[Section, ...],
    footnotes: tuple[str, ...] = (),
) -> str:
    """One HTML document. No external requests of any kind."""
    body = []
    for section in sections:
        if not section.blocks:
            continue
        wrapper = {"tiles": "viz-tiles", "grid": "viz-grid-2", "flow": "viz-flow"}[section.layout]
        body.append(f"<h2>{escape(section.heading)}</h2>")
        body.append(f'<div class="{wrapper}">' + "".join(section.blocks) + "</div>")

    notes = "".join(f"<p>{escape(n)}</p>" for n in footnotes)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title>
<style>{_STYLE}</style>
</head>
<body>
<div class="viz-root">
<header class="viz-head">
  <h1>{escape(title)}</h1>
  <span class="viz-meta">{escape(database)} · generated {escape(generated_at)} UTC</span>
</header>
<p class="viz-lede">{escape(lede)}</p>
{"".join(body)}
<footer class="viz-foot">{notes}</footer>
</div>
</body>
</html>"""
