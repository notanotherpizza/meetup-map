# ATO demo kit: "Letting Agents Cook Without Burning Your Budget"

Per-agent framing: one **limited-scope agent** does triage; everything else is scripted.

| Stage | File | Who | Tokens |
|---|---|---|---|
| 1. Find the gap (64% of listed groups never scraped) | `01_backlog.py` | script | 0 |
| 2. Triage a sample of the backlog | `02_agent_triage.py` | agent (Haiku 4.5, 2 tools) | metered live |
| 3. Build the scrape queue from the triage | `03_queue.py` | script | 0 |
| Record the real site | `capture.py` | script | 0 |

Stage 2 prints a running cost and extrapolates to the whole backlog, so the
"run once / weekly / 1000 runs" slide has real numbers.

```bash
python demo/01_backlog.py
ANTHROPIC_API_KEY=... python demo/02_agent_triage.py --sample 25
python demo/03_queue.py
python demo/capture.py --tag after      # re-run once the index has filled in
```

`asciinema rec -c "python demo/01_backlog.py" demo/out/01_backlog.cast` records a terminal stage.
Needs `pip install playwright anthropic` (and `-e .` for the repo's own scraper).
