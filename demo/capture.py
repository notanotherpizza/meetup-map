"""Capture screenshots + a screen recording of search.notanother.pizza.

    python demo/capture.py [--tag before] [--exe /path/to/chrome-headless-shell]
Outputs to demo/out/<tag>-*.png and demo/out/<tag>-search.webm
"""
import argparse
import json
from pathlib import Path

from playwright.sync_api import sync_playwright

SITE = "https://search.notanother.pizza"
OUT = Path(__file__).resolve().parent / "out"
QUERIES = ["kafka", "python AND london", "data engineering"]


def main(tag: str, exe: str | None) -> None:
    OUT.mkdir(exist_ok=True)
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path=exe) if exe else p.chromium.launch()
        ctx = b.new_context(viewport={"width": 1440, "height": 900},
                            record_video_dir=str(OUT / "_video"), record_video_size={"width": 1440, "height": 900})
        pg = ctx.new_page()
        stats = ctx.request.get(f"{SITE}/stats.json").json()
        (OUT / f"{tag}-stats.json").write_text(json.dumps(stats))
        print("stats:", stats)
        pg.goto(SITE, wait_until="networkidle")
        pg.wait_for_timeout(3000)
        pg.screenshot(path=str(OUT / f"{tag}-home.png"))
        for i, q in enumerate(QUERIES):
            pg.fill("#search-bar-input", "")
            pg.type("#search-bar-input", q, delay=70)
            pg.click("#qb-search-btn")
            pg.wait_for_timeout(3000)
            print(f"{q!r}: {pg.inner_text('#stats-text')}")
            pg.screenshot(path=str(OUT / f"{tag}-search-{i + 1}.png"))
        pg.click("#btn-map")
        pg.wait_for_timeout(3000)
        pg.screenshot(path=str(OUT / f"{tag}-map.png"))
        video = pg.video
        ctx.close()
        video.save_as(str(OUT / f"{tag}-search.webm"))
        b.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="before")
    ap.add_argument("--exe")
    a = ap.parse_args()
    main(a.tag, a.exe)
