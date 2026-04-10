# meetup-map

A community-maintained index of meetup groups and events, searchable at **[search.notanother.pizza](https://search.notanother.pizza)**.

The index is built by a distributed scraper. Anyone can contribute by submitting a group to be listed, or by running a worker to help scrape groups faster.

---

## Add your group

The easiest way to get listed. No coding required — just edit one text file and open a pull request.

### Step-by-step (first time on GitHub)

**1. Create a GitHub account**

Go to [github.com](https://github.com) and sign up for a free account if you don't have one.

**2. Fork this repository**

Click the **Fork** button at the top right of this page. This creates your own copy of the project under your GitHub account.

**3. Edit `community/groups.txt`**

In your forked repo, navigate to `community/groups.txt` and click the pencil icon (✏️) to edit it.

Add your group URL on a new line:

```
https://www.meetup.com/your-group-name/
https://lu.ma/your-calendar-slug
```

One URL per line. Both Meetup and Luma are supported. The URL should be the public page for your group or calendar.

**4. Commit the change**

Scroll down to the **Commit changes** section. Add a short description like `Add PyData Bristol` and click **Commit changes**.

**5. Open a pull request**

Click **Contribute → Open pull request** at the top of your forked repo. Add a brief description of your group and click **Create pull request**.

**6. Wait for review**

Once the PR is merged your group will be scraped and appear in the search index within 24 hours.

**Supported platforms:**
- [Meetup.com](https://meetup.com) — any public group
- [Luma](https://lu.ma) — any public calendar

---

## Run a worker

Workers scrape group pages and publish the results to the shared message queue. Running a worker helps the index stay fresh and increases scraping capacity. The more workers running, the faster new groups get indexed.

Total workers from last run: 1

### What is a worker?

A worker is a small Python process that:
1. Picks up a group from a queue
2. Scrapes its events and metadata from Meetup or Luma
3. Publishes the raw results back to the queue for the sink to store

Workers are **stateless**: they don't write to any database directly. All they need is a Kafka connection. This means you can run as many as you like, on any machine, without risking data corruption.

### Why would I want to run one?

- **You run a large meetup network** and want to make sure your groups are scraped frequently and reliably
- **You want to contribute compute** to the project without managing infrastructure
- **You want to extend the scraper**.  The worker architecture makes it easy to add support for new platforms (Eventbrite, Facebook Events, etc.) by implementing a new `Platform` class
- **You want to index your own platform** . The scraper isn't tied to the community seed. You can bypass the seed producer entirely and publish your own `GroupSeed` messages to the Kafka topic from your own system. This means any platform that can produce a group URL and basic metadata can feed into the index, whether that's a custom events platform, a university society system, or anything else

### Request access

To run a worker you need credentials for the shared Aiven Kafka instance. Open an issue or message in the [notanother.pizza Discord](https://discord.notanother.pizza) to request access. You will receive a `.env` file containing the Kafka connection details.

Note: **the sink (database writer) is not available to community runners** the data contracts (and rate limiting) are enforced at the sink level.

### Set up

```bash
git clone https://github.com/notanotherpizza/meetup-map
cd meetup-map
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

Copy your `.env` file into the repo root.

### Run

```bash
source .env
python -m worker.scraper
```

Workers are stateless — run as many as you want. They share a Kafka consumer group so work is automatically distributed between them.

```bash
# Run three workers in parallel
source .env
python -m worker.scraper &
python -m worker.scraper &
python -m worker.scraper &
```

### Docker

```bash
docker build -t meetupmap .

# Worker only — all that's needed for community runners
docker run --env-file .env meetupmap python -m worker.scraper
```

---

## How it works

```
Seed producer → [groups-to-scrape] → Workers → [groups-raw]  → Sink → Postgres → Renderer → GitHub Pages
                                              → [venues-raw]  ↗
                                              → [events-raw]  ↗
```

1. The **seed producer** publishes one message per group to the `groups-to-scrape` Kafka topic
2. **Workers** consume those messages, scrape group metadata and events from Meetup or Luma, and publish raw JSON to output topics
3. The **sink consumer** reads the raw topics and upserts into Postgres — this runs centrally and is not available to community runners
4. The **map renderer** queries Postgres and generates the static search and map pages deployed to GitHub Pages

### Adding a new platform

Implement `worker/platforms/base.py`'s `Platform` interface:

```python
class MyPlatform(Platform):
    def can_handle(self, url: str) -> bool:
        return "myplatform.com" in url

    async def scrape(self, seed, browser, http_client, max_past_events, worker_id):
        # fetch data, return ScrapeResult
        ...
```

Register it in `worker/scraper.py` and it will be picked up automatically for any seed URL that matches.