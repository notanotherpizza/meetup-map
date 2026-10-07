"""Stage 3 (script, 0 tokens): turn the agent's triage into a scrape queue.

Priority-1 groups go to the front of the scrape order; everything else keeps
file order. batch_worker.order_by_staleness then handles the rest.
    python demo/03_queue.py
"""
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent / "out"

triage = [json.loads(l) for l in (OUT / "triage.jsonl").read_text().splitlines() if l.strip()]
backlog = json.loads((OUT / "backlog.json").read_text())
rank = {t["url"]: t["priority"] for t in triage}
queue = sorted(backlog, key=lambda u: rank.get(u, 2))
(OUT / "queue.txt").write_text("\n".join(queue) + "\n")
print(f"{len(triage)} triaged by the agent -> {sum(1 for t in triage if t['priority'] == 1)} promoted to the front of {len(queue):,}")
