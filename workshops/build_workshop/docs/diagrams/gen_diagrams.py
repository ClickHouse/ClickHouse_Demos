#!/usr/bin/env python3
"""Generate the workshop architecture and module-flow SVGs.

Hand-layout on a grid so nothing overlaps, with a restrained ClickHouse-brand
palette (near-black background, one yellow accent, muted zone tints). Emits
plain SVG (no draw.io round-trip) so the published images are fully legible.
"""
from html import escape

# --- palette ---------------------------------------------------------------
BG        = "#17171A"   # near-black canvas
INK       = "#EDEDED"   # primary text
SUBINK    = "#B7BCC2"   # secondary text / edge labels
YELLOW    = "#FAFF69"   # ClickHouse accent
EDGE      = "#7E858E"   # edge stroke
BOX_FILL  = "#26262B"
BOX_STROKE= "#4C4C55"
FONT      = "Inter, -apple-system, Segoe UI, Roboto, sans-serif"
MONO      = "Inconsolata, ui-monospace, monospace"

# zone tints (muted, harmonious)
ZONES = {
    "laptop": {"fill": "#1B2531", "stroke": "#3E5A78", "label": "PARTICIPANT LAPTOP  ·  the NYC-taxi app runs here"},
    "cloud":  {"fill": "#211F14", "stroke": YELLOW,    "label": "CLICKHOUSE CLOUD  ·  your trial org (you create all of this)"},
    "third":  {"fill": "#1E1E22", "stroke": "#54545C", "label": "THIRD-PARTY"},
}


def _svg_header(w, h):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'viewBox="0 0 {w} {h}" font-family="{FONT}">\n'
        f'<rect x="0" y="0" width="{w}" height="{h}" fill="{BG}"/>\n'
        f'<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="7" refY="3" '
        f'orient="auto" markerUnits="userSpaceOnUse">'
        f'<path d="M0,0 L7,3 L0,6 Z" fill="{EDGE}"/></marker></defs>\n'
    )


def box(x, y, w, h, title, lines=None, accent=False, rounded=12, fill=BOX_FILL, dashed=False):
    stroke = YELLOW if accent else BOX_STROKE
    sw = 2 if accent else 1.3
    dash = ' stroke-dasharray="5 4"' if dashed else ''
    s = (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rounded}" '
         f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{dash}/>\n')
    cx = x + w / 2
    # title
    ty = y + (22 if lines else h/2 + 5)
    s += (f'<text x="{cx}" y="{ty}" fill="{INK}" font-size="15" font-weight="600" '
          f'text-anchor="middle">{escape(title)}</text>\n')
    if lines:
        for i, ln in enumerate(lines):
            s += (f'<text x="{cx}" y="{y+44+i*18}" fill="{SUBINK}" font-size="12.5" '
                  f'text-anchor="middle">{escape(ln)}</text>\n')
    return s


def zone(x, y, w, h, key):
    z = ZONES[key]
    s = (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{z["fill"]}" '
         f'stroke="{z["stroke"]}" stroke-width="1.6" opacity="0.96"/>\n')
    s += (f'<text x="{x+18}" y="{y+26}" fill="{z["stroke"] if key!="cloud" else YELLOW}" '
          f'font-size="13" font-weight="700" letter-spacing="0.5">{escape(z["label"])}</text>\n')
    return s


def edge(x1, y1, x2, y2, label=None, mid=None, dashed=False, label_dx=0, label_dy=-6):
    dash = ' stroke-dasharray="6 5"' if dashed else ''
    s = (f'<path d="M{x1},{y1} L{x2},{y2}" fill="none" stroke="{EDGE}" '
         f'stroke-width="1.6"{dash} marker-end="url(#arrow)"/>\n')
    if label:
        lx = (x1 + x2) / 2 + label_dx if mid is None else mid[0]
        ly = (y1 + y2) / 2 + label_dy if mid is None else mid[1]
        w = 7.2 * len(label) + 10
        s += (f'<rect x="{lx-w/2}" y="{ly-13}" width="{w}" height="18" rx="4" '
              f'fill="{BG}" opacity="0.92"/>\n')
        s += (f'<text x="{lx}" y="{ly}" fill="{SUBINK}" font-size="11.5" '
              f'text-anchor="middle">{escape(label)}</text>\n')
    return s


def elbow(pts, label=None, mid=None, dashed=False):
    """Orthogonal polyline through pts=[(x,y),...], arrow at the end."""
    dash = ' stroke-dasharray="6 5"' if dashed else ''
    d = "M" + " L".join(f"{x},{y}" for x, y in pts)
    s = (f'<path d="{d}" fill="none" stroke="{EDGE}" stroke-width="1.6"{dash} '
         f'marker-end="url(#arrow)"/>\n')
    if label and mid:
        w = 7.2 * len(label) + 10
        s += (f'<rect x="{mid[0]-w/2}" y="{mid[1]-13}" width="{w}" height="18" rx="4" '
              f'fill="{BG}" opacity="0.92"/>\n')
        s += (f'<text x="{mid[0]}" y="{mid[1]}" fill="{SUBINK}" font-size="11.5" '
              f'text-anchor="middle">{escape(label)}</text>\n')
    return s


# ===========================================================================
# MODULE FLOW  (clean two-row snake, uniform on-brand boxes)
# ===========================================================================
def module_flow():
    mods = [
        ("00 Setup", "25 min", "accounts, keys, tools,", "MCP, app up"),
        ("01 ClickHouse Cloud", "15 min", "schema + seed 3.2M", "TLC rows; dashboards light up"),
        ("02 Base app", "5 min", "tour the seeded app:", "dashboards, chat, data flow"),
        ("03 Realtime CDC", "20 min", "own Postgres + ClickPipe", "Ops dashboard goes live"),
        ("04 ClickHouse Agents", "10 min", "conversational BI", "over your taxi data"),
        ("05 ClickStack", "15 min", "OTel overlay: traces", "+ logs in HyperDX"),
        ("06 AI SRE", "15 min", "agent + ClickStack MCP", "builds dashboard + alert"),
        ("07 Chat + Langfuse", "15 min", "in-app AI chat,", "every turn traced"),
        ("08 Break and fix", "20 min", "inject a fault, diagnose", "with the AI SRE, fix it"),
        ("09 Wrap-up", "10 min", "running prototype,", "take it home"),
    ]
    cols, bw, bh = 5, 300, 96
    gx, gy = 40, 40
    mx, my = 44, 70
    W = gx*2 + cols*bw + (cols-1)*mx
    rows = (len(mods) + cols - 1) // cols
    H = gy + 60 + rows*bh + (rows-1)*my + 40

    s = _svg_header(W, H)
    s += (f'<text x="{gx}" y="{gy+8}" fill="{INK}" font-size="20" font-weight="700">'
          f'ClickHouse BUILD Workshop · Module flow</text>\n')
    s += (f'<text x="{gx}" y="{gy+30}" fill="{SUBINK}" font-size="13">'
          f'~2h30 hands-on · modules 00-07 and 09 stay on build-workshop-v1; only module 08 switches branches</text>\n')

    top = gy + 52
    pos = {}
    for i, m in enumerate(mods):
        r = i // cols
        c = i % cols
        # snake: even rows L->R, odd rows R->L
        cc = c if r % 2 == 0 else (cols - 1 - c)
        x = gx + cc*(bw+mx)
        y = top + r*(bh+my)
        pos[i] = (x, y, x+bw, y+bh)
        # box with a yellow accent left bar
        s += (f'<rect x="{x}" y="{y}" width="{bw}" height="{bh}" rx="12" '
              f'fill="{BOX_FILL}" stroke="{BOX_STROKE}" stroke-width="1.3"/>\n')
        s += f'<rect x="{x}" y="{y}" width="5" height="{bh}" rx="2.5" fill="{YELLOW}"/>\n'
        s += (f'<text x="{x+18}" y="{y+26}" fill="{INK}" font-size="14.5" font-weight="700">'
              f'{escape(m[0])}</text>\n')
        s += (f'<text x="{x+bw-14}" y="{y+26}" fill="{YELLOW}" font-size="12.5" '
              f'font-weight="600" text-anchor="end">{escape(m[1])}</text>\n')
        s += (f'<text x="{x+18}" y="{y+52}" fill="{SUBINK}" font-size="12.5">{escape(m[2])}</text>\n')
        s += (f'<text x="{x+18}" y="{y+70}" fill="{SUBINK}" font-size="12.5">{escape(m[3])}</text>\n')

    # arrows following the snake
    for i in range(len(mods)-1):
        ax1, ay1, ax2, ay2 = pos[i]
        bx1, by1, bx2, by2 = pos[i+1]
        r = i // cols
        if pos[i+1][1] == pos[i][1]:  # same row
            if r % 2 == 0:  # L->R
                s += edge(ax2, ay1+bh/2, bx1-6, by1+bh/2)
            else:           # R->L
                s += edge(ax1, ay1+bh/2, bx2+6, by1+bh/2)
        else:  # drop to next row (vertical connector on the turnaround side)
            cxm = (ax1+ax2)/2
            s += elbow([(cxm, ay2), (cxm, by1-6)])
    # note in the clear gap between the two rows (left-aligned, clear of the turnaround arrow)
    ny = top + bh + my/2 + 5
    s += (f'<text x="{gx}" y="{ny}" fill="{SUBINK}" font-size="12" font-style="italic">'
          f'After module 03: historical + live data both flowing into your Cloud service.</text>\n')
    s += "</svg>\n"
    return s


# ===========================================================================
# ARCHITECTURE  (3 zones, aligned rows, top bus for third-party API calls)
# ===========================================================================
def architecture():
    W, H = 1700, 1000
    s = _svg_header(W, H)
    s += (f'<text x="30" y="44" fill="{INK}" font-size="22" font-weight="700">'
          f'ClickHouse BUILD Workshop · Architecture (target end state)</text>\n')
    s += (f'<text x="30" y="70" fill="{SUBINK}" font-size="13.5">'
          f'Everything runs on the participant laptop and their own ClickHouse Cloud trial. '
          f'Dashed lines are control/OAuth; solid lines are data.</text>\n')

    # zones
    s += zone(30, 135, 515, 700, "laptop")
    s += zone(590, 135, 650, 760, "cloud")
    s += zone(1290, 135, 380, 430, "third")

    # --- laptop boxes ---
    s += box(60, 190, 455, 62, "frontend", ["React SPA · nginx :8080", "Ops + Historical dashboards, chat"])
    s += box(60, 300, 455, 62, "backend", ["FastAPI :8000", "analytics API + /api/chat (NL-to-SQL)"])
    s += box(60, 410, 455, 56, "otel-collector", ["ClickStack overlay (module 05)"])
    s += box(60, 520, 455, 62, "pg-trip-writer", ["synthetic trips; creates the CDC", "table + publication on first run"])
    s += box(60, 660, 455, 62, "coding agent + clickhousectl", ["Claude Code / Cursor / Codex", "+ ClickHouse skills + docs llms.txt"])

    # --- cloud boxes ---
    s += box(620, 185, 590, 96, "ClickHouse service  :8443 TLS", ["nyc_tlc_data (taxi_trips, taxi_zones,", "views, CDC MV)   +   otel db (logs, traces)"], accent=True)
    s += box(620, 330, 280, 88, "ClickPipes", ["Postgres CDC pipe", "snapshot + stream (~60s)"])
    s += box(930, 330, 280, 88, "ClickStack / HyperDX", ["traces + logs UI", "module 05"])
    s += box(620, 500, 590, 82, "Postgres managed by ClickHouse  :5432 TLS", ["you create it with clickhousectl (module 03) · public.realtime_trips + pub_taxi"])
    s += box(620, 628, 280, 74, "Remote MCP", ["/mcp + /clickstack (OAuth)"])
    s += box(930, 628, 280, 74, "ClickHouse Agents", ["ai.clickhouse.cloud (module 04)"])
    s += box(620, 748, 590, 60, "Instructor fallback only", ["if a trial org cannot create a managed Postgres, use the shared pool"], dashed=True, fill="#1C1B14")

    # --- third-party boxes ---
    s += box(1315, 195, 330, 74, "Langfuse Cloud", ["chat traces, sessions, cost"])
    s += box(1315, 300, 330, 74, "OpenAI API", ["chat completions (gpt-5.4-mini)"])
    s += box(1315, 410, 330, 88, "NYC TLC dataset", ["public parquet; read once by the", "module 01 seed. Data source only."])

    # --- edges (laptop internal) ---
    s += edge(287, 252, 287, 300, "/api proxy", label_dx=42)
    s += edge(180, 362, 180, 410, "OTLP", label_dx=-34)

    # laptop -> cloud (near-horizontal, labels in the gap)
    s += elbow([(515, 331), (560, 331), (560, 250), (620, 250)], "SQL · TLS :8443", mid=(567, 300))
    s += elbow([(515, 438), (585, 438), (585, 262), (620, 262)], "traces → otel db", mid=(585, 400), dashed=False)
    s += edge(515, 545, 620, 541, "INSERT trips · TLS", label_dy=-8)
    s += edge(515, 690, 620, 665, "MCP · OAuth", label_dy=-8)

    # cloud internal (CDC chain + reads)
    s += edge(760, 500, 760, 418, "logical replication", label_dx=0, label_dy=-4)
    s += edge(760, 330, 760, 281, "CDC rows ~60s", label_dy=-4)
    s += edge(1070, 330, 1070, 281, "reads otel db", label_dy=-4)
    # agents -> service (RBAC), up the cloud's right margin
    s += elbow([(1070, 628), (1215, 628), (1215, 235), (1210, 235)], "RBAC SQL", mid=(1215, 470))

    # third-party: backend -> OpenAI / Langfuse along the top bus (above zones)
    s += elbow([(360, 300), (360, 118), (1480, 118), (1480, 300)], "chat completions", mid=(950, 112))
    s += elbow([(330, 300), (330, 96), (1430, 96), (1430, 195)], "chat traces (Langfuse SDK)", mid=(760, 90))
    # TLC seed -> service (in the cloud/third gap)
    s += elbow([(1315, 452), (1262, 452), (1262, 215), (1210, 215)], "url() seed", mid=(1262, 305))

    s += "</svg>\n"
    return s


# ===========================================================================
# CLICKHOUSE CLOUD PLATFORM  (layered stack, from the demohouse.cloud home page)
# ===========================================================================
def platform_stack():
    W, H = 1060, 720
    LX, LW = 90, 880
    s = _svg_header(W, H)
    s += (f'<text x="{W/2}" y="46" fill="{INK}" font-size="22" font-weight="700" '
          f'text-anchor="middle">The ClickHouse Cloud platform</text>\n')
    s += (f'<text x="{W/2}" y="72" fill="{SUBINK}" font-size="13.5" text-anchor="middle">'
          f'One platform, bottom to top: ingest → store &amp; analyze → observe &amp; add AI. '
          f'The workshop builds on exactly these pieces.</text>\n')

    def layer(y, h, label):
        t = (f'<rect x="{LX}" y="{y}" width="{LW}" height="{h}" rx="12" fill="#1E1E24" '
             f'stroke="#3A3A45" stroke-width="1.3"/>\n')
        if label:
            t += (f'<text x="{LX+16}" y="{y+h/2+4}" fill="{SUBINK}" font-size="11" '
                  f'font-weight="700" letter-spacing="1.5">{label}</text>\n')
        return t

    def band(y, h, spans, dashed=True):
        dash = ' stroke-dasharray="5 4"' if dashed else ''
        t = (f'<rect x="{LX}" y="{y}" width="{LW}" height="{h}" rx="10" fill="#191920" '
             f'stroke="#3A3A45" stroke-width="1.2"{dash}/>\n')
        n = len(spans)
        for i, sp in enumerate(spans):
            cx = LX + LW*(i+0.5)/n
            t += (f'<text x="{cx}" y="{y+h/2+4}" fill="{SUBINK}" font-size="11.5" '
                  f'font-weight="600" letter-spacing="1.2" text-anchor="middle">{sp}</text>\n')
        return t

    def pnode(x, y, w, h, label, sublabel, yellow=False):
        fill = YELLOW if yellow else "#33333F"
        tcol = "#141410" if yellow else INK
        scol = "#141410CC" if yellow else SUBINK
        t = (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="9" fill="{fill}" '
             f'stroke="{"#E4E85F" if yellow else "#50505C"}" stroke-width="1.3"/>\n')
        t += (f'<text x="{x+16}" y="{y+h/2-3}" fill="{tcol}" font-size="14.5" '
              f'font-weight="700">{escape(label)}</text>\n')
        t += (f'<text x="{x+16}" y="{y+h/2+15}" fill="{scol}" font-size="11">{escape(sublabel)}</text>\n')
        return t

    def up_arrow(cx, y_from, y_to):
        # yellow arrow pointing UP (data flows up the stack)
        return (f'<path d="M{cx},{y_from} L{cx},{y_to+7}" stroke="{YELLOW}" stroke-width="2.4" '
                f'fill="none"/><polygon points="{cx-6},{y_to+7} {cx+6},{y_to+7} {cx},{y_to}" fill="{YELLOW}"/>\n')

    cx = LX + LW/2
    # top: APPs / Users / API
    y = 98
    s += band(y, 40, ["APPS", "USERS", "API"])
    s += up_arrow(cx, 190, 142)
    # UIs layer
    y = 196; h = 104
    s += layer(y, h, "UIs")
    nx, nw, ng = LX+78, 196, 16
    s += pnode(nx,            y+22, nw, 60, "LibreChat", "agentic AI chat")
    s += pnode(nx+(nw+ng),   y+22, nw, 60, "HyperDX", "system observability")
    s += pnode(nx+2*(nw+ng), y+22, nw, 60, "Langfuse", "LLM observability")
    # LLMs sidebar (dashed)
    lx = nx+3*(nw+ng)
    s += (f'<rect x="{lx}" y="{y+22}" width="{LX+LW-16-lx}" height="60" rx="9" fill="#191920" '
          f'stroke="#3A3A45" stroke-width="1.2" stroke-dasharray="5 4"/>\n')
    s += (f'<text x="{lx+(LX+LW-16-lx)/2}" y="{y+46}" fill="{SUBINK}" font-size="11" '
          f'font-weight="700" letter-spacing="1.2" text-anchor="middle">LLMs</text>\n')
    s += (f'<text x="{lx+(LX+LW-16-lx)/2}" y="{y+64}" fill="{SUBINK}" font-size="10.5" '
          f'text-anchor="middle">OpenAI / Claude / Gemini</text>\n')
    s += up_arrow(cx, 340, 300)
    # DBs layer
    y = 346; h = 100
    s += layer(y, h, "DBs")
    s += pnode(LX+78, y+22, 240, 58, "Postgres", "OLTP source", yellow=True)
    s += pnode(LX+78+240+16, y+22, LX+LW-16-(LX+78+240+16), 58, "ClickHouse", "OLAP + real-time analytics (the core)", yellow=True)
    s += up_arrow(cx, 486, 446)
    # Stream layer
    y = 492; h = 62
    s += layer(y, h, "STREAM")
    s += pnode(LX+78, y+14, LX+LW-16-(LX+78), 34, "ClickPipes", "managed ingestion · Postgres CDC, Kafka, S3, …")
    s += up_arrow(cx, 594, 554)
    # Sources
    y = 600
    s += band(y, 40, ["OPEN TABLE FORMATS", "DATA LAKE", "OTHER SOURCES"])
    s += "</svg>\n"
    return s


import sys
if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("platform", "all"):
        open("clickhouse-platform.svg", "w").write(platform_stack())
        print("wrote clickhouse-platform.svg")
    if which in ("module", "all"):
        open("workshop-module-flow.svg", "w").write(module_flow())
        print("wrote workshop-module-flow.svg")
    if which in ("arch", "all"):
        open("workshop-architecture.svg", "w").write(architecture())
        print("wrote workshop-architecture.svg")
