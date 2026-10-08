"""Render demo/out/backfill-progress.png: groups added per hour since the scraper fix.
Data = result of the query in demo/managed-agent/queries.sql (section 4), pasted below."""
import argparse
from pathlib import Path
from playwright.sync_api import sync_playwright

# (hour label UTC, new groups) from catalog-db, 2026-10-07 11:00 -> 2026-10-08 10:00 (last hour partial)
HOURS = [("11",470),("12",933),("13",970),("14",916),("15",952),("16",1200),("17",1172),("18",1133),
         ("19",1138),("20",1113),("21",1153),("22",1152),("23",1160),("00",1222),("01",1279),("02",1127),
         ("03",1033),("04",1133),("05",1180),("06",1152),("07",1145),("08",1173),("09",1120)]
BEFORE, AFTER = 16_9, 42_068

def svg() -> str:
    w, h, left, bottom, top = 1280, 420, 70, 60, 20
    mx = 1400; bw = (w - left - 20) / len(HOURS)
    bars, labels = "", ""
    for i, (hr, n) in enumerate(HOURS):
        bh = (h - bottom - top) * n / mx; x = left + i * bw
        bars += f'<rect x="{x+3:.1f}" y="{h-bottom-bh:.1f}" width="{bw-6:.1f}" height="{bh:.1f}" fill="#1bb2b0"/>'
        labels += f'<text x="{x+bw/2:.1f}" y="{h-bottom+22}" text-anchor="middle">{hr}</text>'
    grid = "".join(f'<line x1="{left}" x2="{w-20}" y1="{h-bottom-(h-bottom-top)*v/mx:.1f}" y2="{h-bottom-(h-bottom-top)*v/mx:.1f}" stroke="#444547"/>'
                   f'<text x="{left-10}" y="{h-bottom-(h-bottom-top)*v/mx+5:.1f}" text-anchor="end">{v:,}</text>' for v in (0,400,800,1200))
    return f'<svg viewBox="0 0 {w} {h}" width="{w}" height="{h}" font-size="15" fill="#b5b5b7">{grid}{bars}{labels}' \
           f'<text x="{left+(w-left)/2}" y="{h-8}" text-anchor="middle">hour (UTC), 7 Oct 11:00 to 8 Oct 09:00</text></svg>'

HTML = f"""<body style="margin:0;background:#16171a;font-family:Inter,system-ui,sans-serif;color:#f4f4f4;width:1280px;padding:36px 0">
<div style="padding:0 40px"><div style="font:600 14px ui-monospace;letter-spacing:.18em;color:#1bb2b0">NOT ANOTHER PIZZA · BACKFILL</div>
<h1 style="font-size:40px;margin:8px 0 4px">17k to 42k groups in 24 hours</h1>
<div style="font-size:20px;color:#b5b5b7;margin-bottom:12px">New groups scraped per hour after the scraper fix went live (about 1,150 an hour, steady)</div></div>
<div style="padding:0 0 0 0">{svg()}</div></body>"""

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--exe"); a = ap.parse_args()
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path=a.exe) if a.exe else p.chromium.launch()
        pg = b.new_page(viewport={"width":1280,"height":640}); pg.set_content(HTML)
        pg.screenshot(path=str(Path(__file__).parent/"out"/"backfill-progress.png")); b.close()
