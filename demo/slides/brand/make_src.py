"""Generate templates/ato.src.html (Aiven brand deck, 9 slides) from the case-study content."""
import re
from pathlib import Path

ROOT = Path(__file__).parent
tpl = (ROOT / "templates" / "brand-deck.html").read_text()
head = tpl[: tpl.index("<deck-stage")]
tail = tpl[tpl.index("</deck-stage>") :]

head = head.replace("<title>Aiven deck</title>", "<title>Not Another Pizza case study</title>")
extra_css = """
  .stat { font-family: var(--font-display); font-weight: 800; text-transform: uppercase; color: var(--brand-green); line-height: 0.9; letter-spacing: -0.01em; }
  .shot { display: block; border: 1px solid #1F2430; }
  .cap  { font-family: var(--font-sans); font-weight: 400; font-size: 22px; line-height: 1.4; color: rgba(255,255,255,0.55); margin-top: 20px; }
  .cap strong { color: #FFFFFF; font-weight: 600; }
  .split { display: flex; gap: 80px; flex: 1; min-height: 0; align-items: flex-start; }
  .lead { font-family: var(--font-sans); font-weight: 400; font-size: 30px; line-height: 1.4; color: #FFFFFF; margin-top: 32px; }
"""
head = head.replace("</style>", extra_css + "</style>", 1)


def section(label, inner, cls="slide", syms=""):
    return f'  <section class="{cls}" data-label="{label}">\n{syms}    <div class="frame">\n{inner}    </div>\n  </section>\n\n'


S = []

# 1 section title
S.append(
    f'  <section class="slide section-title" data-label="Case study">\n'
    '    <img class="sym" src="../assets/symbols/Teal/Curly brace right.svg" alt="" style="right:-100px;top:-40px;height:1160px;">\n'
    '    <img class="sym" src="../assets/symbols/Purple/Slash.svg" alt="" style="left:200px;bottom:-80px;height:440px;">\n'
    '    <div class="frame" style="justify-content:center;">\n'
    '      <div class="eyebrow">// CASE STUDY</div>\n'
    '      <h2 class="h-section">Not Another<br>Pizza</h2>\n'
    '      <div class="lead" style="max-width:1000px;">A script for the boring parts.<br>An agent for the judgement.</div>\n'
    "    </div>\n  </section>\n\n"
)

# 2 problem
S.append(section("The problem", """      <div class="eyebrow">// THE PROBLEM</div>
      <h2 class="h-compact">64% of the index was missing</h2>
      <div class="split">
        <div style="width:700px;flex-shrink:0;">
          <div class="stat" style="font-size:240px;">64%</div>
          <div class="lead">27,981 of 43,425 listed groups had never been scraped, so searches could not find them.</div>
        </div>
        <div>
          <img class="shot" src="../work/before-search-1.png" alt="" style="width:900px;">
          <div class="cap"><strong>Before:</strong> 15,442 groups indexed. "kafka" found 13.</div>
        </div>
      </div>
"""))

# 3-5 video slides
def video_slide(label, eyebrow, headline, poster, num, num_size, body, cap):
    return section(label, f"""      <div class="eyebrow">// {eyebrow}</div>
      <h2 class="h-compact">{headline}</h2>
      <div class="split">
        <div style="flex-shrink:0;">
          <img class="shot" src="../work/{poster}" alt="" style="width:1040px;">
          <div class="cap">{cap}</div>
        </div>
        <div>
          <div class="stat" style="font-size:{num_size}px;">{num}</div>
          <div class="lead" style="font-size:28px;">{body}</div>
        </div>
      </div>
""")

S.append(video_slide("Script finds the gap", "THE SCRIPT", "A script found the gap", "poster-01_backlog.png", "0.1S", 150,
                     "0 tokens. Same answer on the thousandth run.", "Compare the group list with the index. No model needed."))
S.append(video_slide("Agent triage", "THE AGENT", "Where an agent earns its keep", "poster-02_agent_triage.png", "$0.07", 150,
                     "25 groups triaged. Two tools, one job, read-only.", "The one step that needs judgement: which groups first?"))
S.append(video_slide("Script acts", "THE HAND-OFF", "Then a script acts on the answer", "poster-03_queue.png", "4 OF 25", 110,
                     "moved to the front of the queue. The agent decides. Code does the work.", "The agent returns a list. A script builds the queue."))

# 6 table
S.append(section("Cost by cadence", """      <div class="eyebrow">// THE COST</div>
      <h2 class="h-compact">What it costs, by how often you ask</h2>
      <table class="brand">
        <thead><tr><th>Cadence</th><th>Agent triage</th><th>Script</th></tr></thead>
        <tbody>
          <tr><td>Once, 25 groups</td><td>$0.07</td><td>$0</td></tr>
          <tr><td>1,000 runs of 25</td><td>about $74</td><td>$0</td></tr>
          <tr><td>Whole backlog, 27,981 groups</td><td>about $82</td><td>$0</td></tr>
        </tbody>
      </table>
      <div class="cap" style="margin-top:32px;">Haiku 4.5 list price, measured on 25 groups and extrapolated. The Aiven AI gateway meters the exact figure per key.</div>
"""))

# 7 chart
vals = [470, 933, 970, 916, 952, 1200, 1172, 1133, 1138, 1113, 1153, 1152, 1160, 1222, 1279, 1127, 1033, 1133, 1180, 1152, 1145, 1173, 1120]
hours = ["11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "00", "01", "02", "03", "04", "05", "06", "07", "08", "09"]
W, H, L, B, T = 1720, 520, 90, 50, 10
mx = 1400
bw = (W - L) / len(vals)
bars = ""
for i, (h_, v) in enumerate(zip(hours, vals)):
    bh = (H - B - T) * v / mx
    x = L + i * bw
    bars += f'<rect x="{x + 6:.1f}" y="{H - B - bh:.1f}" width="{bw - 12:.1f}" height="{bh:.1f}" fill="#5FFA74"/>'
    bars += f'<text x="{x + bw / 2:.1f}" y="{H - B + 32}" text-anchor="middle">{h_}</text>'
grid = ""
for v in (0, 400, 800, 1200):
    y = H - B - (H - B - T) * v / mx
    grid += f'<line x1="{L}" x2="{W}" y1="{y:.1f}" y2="{y:.1f}" stroke="rgba(255,255,255,0.12)"/><text x="{L - 16}" y="{y + 6:.1f}" text-anchor="end">{v:,}</text>'
svg = f'<svg viewBox="0 0 {W} {H}" width="{W}" height="{H}" style="font-family:var(--font-mono);font-size:18px;fill:rgba(255,255,255,0.55);">{grid}{bars}</svg>'
S.append(section("Backfill", f"""      <div class="eyebrow">// DEPLOYED ON AIVEN</div>
      <h2 class="h-compact" style="margin-bottom:24px;">17K to 42K groups in 24 hours</h2>
      {svg}
      <div class="cap" style="margin-top:12px;">New groups scraped per hour (UTC), 7 Oct 11:00 to 8 Oct 09:00. Steady at about 1,150 an hour.</div>
"""))

# 8 before / after
S.append(section("Result", """      <div class="eyebrow">// THE RESULT</div>
      <h2 class="h-compact">Same search, 2.6x more groups</h2>
      <div class="split" style="gap:60px;">
        <div><img class="shot" src="../work/before-search-3.png" alt="" style="width:830px;"><div class="cap"><strong>Before:</strong> 15,442 groups. "data engineering": 10 results.</div></div>
        <div><img class="shot" src="../work/after-search-3.png" alt="" style="width:830px;"><div class="cap" style="color:#5FFA74;"><strong style="color:#5FFA74;">After:</strong> 40,550 groups. "data engineering": 33 results.</div></div>
      </div>
"""))

# 9 list
S.append(section("Not measured", """      <div class="eyebrow">// THE HONEST BIT</div>
      <h2 class="h-compact">What we did not measure</h2>
      <div class="rows" style="flex:0 0 auto;">
        <div class="row"><div class="dot"></div><div><div class="title">Quality of the agent's picks</div><div class="text">Unchecked. Treat the four priority 1 groups as examples, not a result.</div></div></div>
        <div class="row"><div class="dot"></div><div><div class="title">Cost</div><div class="text">A 25-group sample at list price, extrapolated to the backlog.</div></div></div>
        <div class="row"><div class="dot"></div><div><div class="title">Our first Luma fix</div><div class="text">It was wrong. We caught it by checking dates, not labels.</div></div></div>
      </div>
      <div class="lead" style="margin-top:48px;font-weight:600;">Measure cost + quality, or pay for retries.</div>
"""))

(ROOT / "templates" / "ato.src.html").write_text(head + "<deck-stage width=\"1920\" height=\"1080\">\n\n" + "".join(S) + tail)
print("wrote templates/ato.src.html with", len(S), "slides")
