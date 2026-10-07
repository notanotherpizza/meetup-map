"""Stage 2 (agent, limited scope): triage a sample of the unscraped backlog.

The agent has exactly two tools and one job: look at a group and decide how
soon it deserves a scrape. It never scrapes, writes the DB or touches the
queue; those stay scripted (see 03_queue.py).

    ANTHROPIC_API_KEY=... python demo/02_agent_triage.py --sample 25
"""
import argparse
import asyncio
import json
import random
import sys
from pathlib import Path

import anthropic
import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from worker.platforms import get_platform  # noqa: E402
from worker.scraper import url_to_seed  # noqa: E402

OUT = ROOT / "demo" / "out"
MODEL = "claude-haiku-4-5-20251001"
PRICE_IN, PRICE_OUT = 1.00 / 1e6, 5.00 / 1e6  # USD per token, Haiku 4.5

SYSTEM = """You triage a backlog of meetup groups that have never been scraped.
For each group URL you are given: call peek_group, then call record_triage exactly once.
priority: 1 = scrape first (active, tech/developer/data community, decent size),
2 = normal, 3 = low (social/dating/hobby, tiny, or dormant). Keep reasons under 15 words.
Do nothing else."""

TOOLS = [
    {
        "name": "peek_group",
        "description": "Fetch the group's public page: name, city, member count, description.",
        "input_schema": {"type": "object", "properties": {"url": {"type": "string"}}, "required": ["url"]},
    },
    {
        "name": "record_triage",
        "description": "Record the triage decision for one group.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "priority": {"type": "integer", "enum": [1, 2, 3]},
                "topic": {"type": "string"},
                "reason": {"type": "string"},
            },
            "required": ["url", "priority", "topic", "reason"],
        },
    },
]


async def peek_group(url: str, http: httpx.AsyncClient) -> dict:
    try:
        r = await get_platform(url).scrape(
            seed=url_to_seed(url, None), http_client=http, max_past_events=0, worker_id="triage-agent"
        )
        g = r.group
        return {"name": g.name, "city": g.city, "members": g.member_count,
                "description": (g.description or "")[:400], "upcoming": len(r.upcoming_events)}
    except Exception as exc:  # the agent should see failures, not crash
        return {"error": str(exc)[:200]}


async def main(sample: int, seed: int) -> None:
    backlog = json.loads((OUT / "backlog.json").read_text())
    urls = random.Random(seed).sample(backlog, min(sample, len(backlog)))
    client = anthropic.AsyncAnthropic()
    tokens_in = tokens_out = 0
    out = (OUT / "triage.jsonl").open("w")

    async with httpx.AsyncClient(timeout=30) as http:
        for n, url in enumerate(urls, 1):
            messages = [{"role": "user", "content": f"Triage this group: {url}"}]
            while True:
                resp = await client.messages.create(
                    model=MODEL, max_tokens=512, system=SYSTEM, tools=TOOLS, messages=messages
                )
                tokens_in += resp.usage.input_tokens
                tokens_out += resp.usage.output_tokens
                messages.append({"role": "assistant", "content": resp.content})
                results, done = [], False
                for block in resp.content:
                    if block.type != "tool_use":
                        continue
                    if block.name == "peek_group":
                        content = json.dumps(await peek_group(block.input["url"], http))
                    else:
                        out.write(json.dumps(block.input) + "\n")
                        out.flush()
                        content, done = "recorded", True
                        i = block.input
                        cost = tokens_in * PRICE_IN + tokens_out * PRICE_OUT
                        print(f"[{n:>3}/{len(urls)}] P{i['priority']} {i['topic'][:22]:<22} {i['reason'][:55]:<55} ${cost:.4f}")
                    results.append({"type": "tool_result", "tool_use_id": block.id, "content": content})
                if done or resp.stop_reason != "tool_use":
                    break
                messages.append({"role": "user", "content": results})

    cost = tokens_in * PRICE_IN + tokens_out * PRICE_OUT
    print(f"\n{len(urls)} groups | {tokens_in:,} in / {tokens_out:,} out tokens | ${cost:.4f} "
          f"| ${cost / len(urls):.5f}/group | full backlog ~${cost / len(urls) * len(backlog):,.0f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=25)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    asyncio.run(main(a.sample, a.seed))
