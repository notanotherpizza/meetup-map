"""Stage 1 (script, 0 tokens): which listed groups are missing from the index?

Deterministic, free, and re-runnable weekly or 1000 times.
    python demo/01_backlog.py
"""
import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "demo" / "out"


def urlname(url: str) -> str:
    return url.strip().rstrip("/").split("/")[-1].lower()


def main() -> None:
    t0 = time.time()
    listed = [
        l.strip() for l in (ROOT / "community/groups.txt").read_text().splitlines()
        if l.strip() and not l.startswith("#")
    ]
    indexed = {g["id"].lower() for g in json.loads((ROOT / "docs/data/groups.json").read_text())}
    backlog = [u for u in dict.fromkeys(listed) if urlname(u) not in indexed]

    OUT.mkdir(exist_ok=True)
    (OUT / "backlog.json").write_text(json.dumps(backlog))
    print(f"listed groups   : {len(set(listed)):>7,}")
    print(f"in the index    : {len(indexed):>7,}")
    print(f"never scraped   : {len(backlog):>7,}  ({len(backlog) / len(set(listed)):.0%} of the list)")
    print(f"tokens used     : {0:>7}")
    print(f"wall clock      : {time.time() - t0:>6.2f}s")


if __name__ == "__main__":
    main()
