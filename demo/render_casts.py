"""Render asciinema .cast files in demo/out to .webm videos (and .mp4 if ffmpeg exists).

    python demo/render_casts.py [--exe /path/to/chrome-headless-shell]
Plays each cast in asciinema-player inside headless Chromium and records the page.
"""
import argparse
import json
import functools
import http.server
import shutil
import subprocess
import threading
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parent / "out"
PLAYER = "https://cdn.jsdelivr.net/npm/asciinema-player@3.17.0/dist/bundle"
PAGE = """<!doctype html><meta charset=utf-8>
<link rel=stylesheet href="{p}/asciinema-player.min.css">
<style>html,body{{margin:0;background:#16171a;height:100%}}#d{{padding:24px}}</style>
<div id=d></div><script src="{p}/asciinema-player.min.js"></script>
<script>AsciinemaPlayer.create('{cast}', document.getElementById('d'),
 {{autoPlay:true,fit:'width',theme:'asciinema',terminalFontSize:'16px',idleTimeLimit:2,speed:1}});</script>"""


def duration(cast: Path) -> float:
    """Playback length. v3 casts store the delay since the previous event, so sum them
    (each capped at the player's idle limit)."""
    return sum(min(float(json.loads(line)[0]), 2.0) for line in cast.read_text().splitlines()[1:])


def serve() -> http.server.ThreadingHTTPServer:
    # asciinema-player fetches the cast with XHR, which file:// blocks
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(OUT))
    handler.log_message = lambda *a, **k: None
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def main(exe: str | None) -> None:
    casts = sorted(OUT.glob("0*.cast"))
    srv = serve()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path=exe) if exe else p.chromium.launch()
        for cast in casts:
            html = OUT / f"_{cast.stem}.html"
            html.write_text(PAGE.format(p=PLAYER, cast=cast.name))
            ctx = b.new_context(viewport={"width": 1280, "height": 720},
                                record_video_dir=str(OUT / "_vid"), record_video_size={"width": 1280, "height": 720})
            pg = ctx.new_page()
            pg.goto(f"{base}/{html.name}")
            pg.wait_for_timeout(int((duration(cast) + 3) * 1000))
            video = pg.video
            ctx.close()
            webm = OUT / f"{cast.stem}.webm"
            video.save_as(str(webm))
            html.unlink()
            if shutil.which("ffmpeg"):
                subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(webm), "-pix_fmt", "yuv420p",
                                str(OUT / f"{cast.stem}.mp4")], check=True)
            print("rendered", webm.name)
        b.close()
    srv.shutdown()
    shutil.rmtree(OUT / "_vid", ignore_errors=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--exe")
    main(ap.parse_args().exe)
