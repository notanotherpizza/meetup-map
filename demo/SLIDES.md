# Adding the demo to Jay's deck: "Letting Agents Cook Without Burning Your Budget"

Slide numbers refer to Jay's 28-slide PDF. Rule of thumb: **one asset per slide, a caption under it, no extra slides**
except one optional "what we found" slide. Videos are silent terminal recordings, so Jay narrates over them.

## Where each asset goes
| Slide | Add | Asset (in `demo/out/`) | One-line caption |
|---|---|---|---|
| 3 "Don't leave the stove on" | cost number | text from `02_agent_triage.mp4` last line | "25 groups triaged for $0.07; the full 28k backlog is ~$82" |
| 4 "Continuous iteration trap" (run once / weekly / 1000 runs) | cost table | see table below | "Same job, three cadences" |
| 6-7 Prompt / Skill engineering | nothing new | | the agent run is the *payoff*, not the example of the problem |
| 8 "Why can't I just ask?" | `01_backlog.mp4` | | "Finding the gap took 0.11s and zero tokens. Not an AI problem." |
| 9 "Script" | `01_backlog.mp4` then `03_queue.mp4` | | "Scripts find the gap and act on the answer" |
| 10-11 "Deployable system / Aiven Runtimes" | 3 console screenshots (below) | take by hand | "Same code, running on an Aiven App against Aiven Postgres" |
| 12 "You need data" | `backfill-progress.png` | | "17k -> 42k groups in 24 hours once the scraper was fixed" |
| 13 shoutout (notanotherpizza) | before/after search screenshots | `before-*.png`, then `after-*.png` | "kafka: 13 results before, N after" |
| 25-26 Visibility / Access | the agent definition | `managed-agent/AGENT.md` | "Read-only agent; two kinds of tool; nothing else" |

New slide (optional, after 9): **"Where the agent earns its keep"**: `02_agent_triage.mp4` (53s). Everything before it is scripted;
this is the only step that needs judgement ("is this group worth scraping first?").

## Cost table for slide 4 (measured, Haiku 4.5 list price; replace with the gateway's metered figure)
| Cadence | Groups | Cost |
|---|---|---|
| Run once | 25 | $0.07 |
| Weekly, full backlog | ~28,000 | ~$82 |
| 1000 runs of 25 | 25,000 | ~$74 |
The point: the script costs $0 at every cadence; the agent cost scales with how often you ask it to *think*.

## The three console screenshots to take (Aiven console, project meetup-map)
1. **Apps -> batch -> logs**, showing `[n/43419]` (proves the deploy and the scale).
2. **AI gateway -> AI access keys / usage**, the key's metered cost (replaces the list-price estimate on slide 4).
3. **Managed Agents -> triage agent run** (after you create it from `managed-agent/AGENT.md`), or skip this one.

## Caveats to say out loud (credibility)
- Agent quality was **not** measured: the 4 priority-1 picks are the model's judgement, unverified.
- 25 groups is a sample; the $82 is an extrapolation at list price.
- We caught the bug by querying the data, and the first fix for the Luma scraper was itself wrong. That's an honest "iterate, don't trust" beat for slides 6-8.
