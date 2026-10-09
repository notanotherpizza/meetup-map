// Aiven brand deck (Brand 2.0) for the Not Another Pizza case study. Geometry = 1920x1080 px frame on a 13.333x7.5 in slide (1 px = 1/144 in).
const pptxgen = require("pptxgenjs");
const sharp = require("sharp");
const fs = require("fs");
const path = require("path");

const SK = "/private/tmp/claude-503/-Users-hugh-repos-mapping-pydata/51870eb0-c033-4bb8-91f6-a84f79db329d/scratchpad/brand";
const DEMO = "/Users/hugh/repos/meetup-map/demo";
const OUT = `${DEMO}/slides/ato-case-study.pptx`;
const px = (v) => v / 144;
const pt = (v) => v * 0.75;

const GREEN = "5FFA74", NEAR = "05080F", ROW = "1D1D1F", MUTED = "8D9098", SOFT = "DADBDE", DIV = "22252D", WHITE = "FFFFFF";
const HEAD = "Funnel Display ExtraBold", SANS = "Inter", MONO = "Geist Mono";

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE";
pres.title = "Case study: Not Another Pizza";
pres.author = "Aiven";

const dataUri = (f, mime = "image/png") => `${mime};base64,` + fs.readFileSync(f).toString("base64");
const BG = { path: `${SK}/assets/slide-background.png` };

async function symbol(name, colour, heightPx) {
  const file = `${SK}/assets/symbols/${colour}/${name}.svg`;
  const png = await sharp(fs.readFileSync(file), { density: 300 }).resize({ height: heightPx }).png().toBuffer();
  const meta = await sharp(png).metadata();
  return { data: "image/png;base64," + png.toString("base64"), w: meta.width, h: meta.height };
}

const eyebrow = (s, t, y = 100) =>
  s.addText(`// ${t}`, { x: px(100), y: px(y), w: px(1720), h: px(26), fontFace: MONO, fontSize: pt(18), color: GREEN, charSpacing: 2, isTextBox: true, margin: 0, valign: "top" });
const headline = (s, t, y = 146, size = 56) =>
  s.addText(t.toUpperCase(), { x: px(100), y: px(y), w: px(1720), h: px(size * 1.1), fontFace: HEAD, fontSize: pt(size), color: WHITE, isTextBox: true, margin: 0, valign: "top" });
const body = (s, t, o) =>
  s.addText(t, { fontFace: SANS, isTextBox: true, margin: 0, valign: "top", color: WHITE, ...o });
const caption = (s, runs, o) =>
  s.addText(runs, { fontFace: SANS, fontSize: pt(22), color: MUTED, isTextBox: true, margin: 0, valign: "top", ...o });
const cover = (name) => dataUri(`${DEMO}/slides/poster-${name}.png`);

const NOTES = [
`Okay. So far I've told you a lot of rules. Prompt, then skill, then script, then a deployable system. That's all a bit abstract, so here's a real one.

Hugh built this thing called Not Another Pizza. It's a search index of community meetups, tens of thousands of them. And we used it to try out my whole theory: script the boring bits, and only bring in an agent for the one bit that needs a brain.

Nine slides, about twelve minutes. If I'm running long, I'll skip the two little script videos.

[Fits: straight after the Script slide, before "Skip the line". If your running order changes, it works anywhere after the prompt, skill, script idea has landed. If the videos don't play after import, upload the three mp4s to Drive and use Insert, Video, Google Drive.]`,

`Here's the bit nobody noticed. The index thought it knew about forty three thousand groups. It had actually scraped fifteen. Sixty four percent of them had never been looked at.

And you'd never know. Every search still gives you something back. It looks fine. That's the "you don't know what you're missing" slide from earlier, except now it's real.

Imagine putting a model on top of that. It would answer you with total confidence from about a third of the picture.

[Fits: this is the "you'll need a datastore" point in real life. Numbers are from the 7th of October. The screenshot is the live site before the fix: 15,442 groups, and "kafka" finds 13.]`,

`So how do we find the gap? Do we ask a model? No. It's two lists. You take the groups we know about, you take what's in the index, and you subtract one from the other.

A tenth of a second. Zero tokens. And it gives you the same answer on the thousandth run.

Which is the answer to my "why can't I just ask?" slide. You can. You really shouldn't.

[Video is three seconds and silent. Read the numbers out loud: 43,425 listed, 15,442 in the index, 27,981 never scraped, tokens used zero. Fits right after your "Why can't I just ask" slide, or alongside Script.]`,

`Now here's where I'd actually pay for an agent. We've got twenty eight thousand missing groups. Which do we scrape first? Is this a React Native community or a book club? That's a judgement call. A script can't make it.

So we gave an agent exactly two tools. Look at a group. Write down a priority. That's it. It can't scrape, it can't write to the database, it can't touch the queue.

Watch the counter. That's the stove. Twenty five groups, seven cents. About a third of a cent a group.

Four of them came out priority one. And I'll be straight with you: nobody has checked whether those picks are right. They're examples, not a result.

[Video is fifty three seconds, silent. Fits with the "Don't leave the stove on" and "Continuous Iteration Trap" slides. Model is Claude Haiku 4.5 through the Aiven AI gateway, which meters usage per key.]`,

`And then we go straight back to a script. The agent hands over a list. A script turns that list into a queue and moves the good ones to the front.

The agent decides. Code does the work. If the agent's wrong, the worst thing that happens is a group gets scraped a bit later. That's your "enough access to do the job and no more" slide, just out in the wild.

[Fits with the Access slide in the governance section. This is the easiest one to cut if you're short on time. The first script video already makes the point.]`,

`Remember on the iteration trap slide I said: run it once, run it weekly, run it a thousand times? Here's that table.

The script costs nothing. Every time. The agent is seven cents for twenty five groups. A thousand runs of that is about seventy four dollars. Triaging the whole backlog is about eighty two.

So it's not really agent versus script. It's: which step actually needs to think, and how often are you going to ask it to?

[Honest caveats: this is list price, from a twenty five group sample, stretched out. The Aiven AI gateway shows the real metered cost per key, so swap that number in if it's different.]`,

`Fixing the actual bug was boring. The scraper was working through an old list in file order, and it never got to the new groups. So we changed it to ask the database what it already had, do the unseen ones first, then the stalest.

We put it on an Aiven App, next to Aiven Postgres, and left it running. Seventeen thousand groups to forty two thousand in a day. About eleven hundred an hour, dead steady.

That's the whole point of the deployable system slide. The state lives in the database, so a restart doesn't lose your place.

[Fits with Deployable System and Aiven Runtimes. If you want proof on the slide, add a screenshot of the Aiven console showing the batch app logs, something like "n of 43,419". The first bar is partial because the scraper started halfway through that hour.]`,

`Same site. Same search. Same code. The only thing that changed is the data. "Data engineering" went from ten results to thirty three.

Not everything grew, mind. "Python and London" gave one result before and one after. More data doesn't mean more of everything. But it's the difference between a search that's guessing and one that actually knows.

Which is why I keep going on about data.

[Fits right before the Shoutouts slide, so the thank you to Hugh Evans and search.notanother.pizza lands as the payoff. Figures: groups 15,442 to 40,550, events 829,236 to 2,029,657. The site updates once a day, so it lags the database a little.]`,

`Last one of the block, and it's the honest one. We measured what the agent cost. We did not measure whether it was right.

Those four priority one groups are examples, not a result. The cost is a twenty five group sample at list price, stretched out. And our first fix for the Luma scraper was wrong. It read past events from a page that actually shows upcoming ones, so it labelled future events as past. It looked right until we checked the dates.

So: measure cost, measure quality. Otherwise you pay for retries, and retries are where the money goes.

[Then straight into the "how can companies leverage AI post-hype" section. Luma facts if asked: 746 of 765 Luma groups now have real past events, 37,233 in total, none with a future date.]`,
];

(async () => {
  const brace = await symbol("Curly brace right", "Teal", 1160);
  const slash = await symbol("Slash", "Purple", 440);

  // 1 ─ section title (reference layout: plain bg, symbols fixed) ─────────────
  let s = pres.addSlide();
  s.background = { color: NEAR };
  s.addImage({ data: brace.data, x: px(1920 + 100 - brace.w), y: px(-40), w: px(brace.w), h: px(brace.h) });
  s.addImage({ data: slash.data, x: px(200), y: px(1080 + 80 - slash.h), w: px(slash.w), h: px(slash.h) });
  eyebrow(s, "CASE STUDY", 379);
  s.addText("NOT ANOTHER\nPIZZA", { x: px(100), y: px(425), w: px(1100), h: px(160), fontFace: HEAD, fontSize: pt(80), color: WHITE, isTextBox: true, margin: 0, valign: "top", lineSpacingMultiple: 1.0 });
  body(s, "A script for the boring parts.\nAn agent for the judgement.", { x: px(100), y: px(617), w: px(1000), h: px(90), fontSize: pt(30) });
  s.addNotes(NOTES[0]);

  // 2 ─ the problem ──────────────────────────────────────────────────────────
  s = pres.addSlide(); s.background = BG;
  eyebrow(s, "THE PROBLEM"); headline(s, "64% of the index was missing");
  s.addText("64%", { x: px(100), y: px(250), w: px(700), h: px(216), fontFace: HEAD, fontSize: pt(240), color: GREEN, isTextBox: true, margin: 0, valign: "top" });
  body(s, "27,981 of 43,425 listed groups had never been scraped, so searches could not find them.", { x: px(100), y: px(498), w: px(700), h: px(150), fontSize: pt(30) });
  s.addImage({ path: `${DEMO}/out/before-search-1.png`, x: px(880), y: px(250), w: px(900), h: px(562) });
  caption(s, [{ text: "Before: ", options: { bold: true, color: WHITE } }, { text: '15,442 groups indexed. "kafka" found 13.' }], { x: px(880), y: px(832), w: px(900), h: px(36) });
  s.addNotes(NOTES[1]);

  // 3-5 ─ video slides ───────────────────────────────────────────────────────
  const vid = (n, eye, head, name, mp4, num, numPx, text, cap) => {
    const sl = pres.addSlide(); sl.background = BG;
    eyebrow(sl, eye); headline(sl, head);
    sl.addMedia({ type: "video", path: `${DEMO}/out/${mp4}`, cover: cover(name), x: px(100), y: px(250), w: px(1040), h: px(585) });
    caption(sl, cap, { x: px(100), y: px(855), w: px(1040), h: px(36) });
    sl.addText(num, { x: px(1220), y: px(250), w: px(600), h: px(numPx * 0.95), fontFace: HEAD, fontSize: pt(numPx), color: GREEN, isTextBox: true, margin: 0, valign: "top" });
    body(sl, text, { x: px(1220), y: px(250 + numPx * 0.9 + 32), w: px(600), h: px(200), fontSize: pt(28) });
    sl.addNotes(NOTES[n]);
  };
  vid(2, "THE SCRIPT", "A script found the gap", "01_backlog", "01_backlog.mp4", "0.1S", 150, "0 tokens. Same answer on the thousandth run.", "Compare the group list with the index. No model needed.");
  vid(3, "THE AGENT", "Where an agent earns its keep", "02_agent_triage", "02_agent_triage.mp4", "$0.07", 150, "25 groups triaged. Two tools, one job, read-only.", "The one step that needs judgement: which groups first?");
  vid(4, "THE HAND-OFF", "Then a script acts on the answer", "03_queue", "03_queue.mp4", "4 OF 25", 110, "moved to the front of the queue. The agent decides. Code does the work.", "The agent returns a list. A script builds the queue.");

  // 6 ─ cost table ───────────────────────────────────────────────────────────
  s = pres.addSlide(); s.background = BG;
  eyebrow(s, "THE COST"); headline(s, "What it costs, by how often you ask");
  const border = { type: "solid", pt: 0.75, color: DIV };
  const th = (t) => ({ text: t.toUpperCase(), options: { fontFace: MONO, fontSize: pt(18), color: GREEN, fill: { color: ROW }, border, margin: [0.14, 0.17, 0.14, 0.17] } });
  const td = (t, first) => ({ text: t, options: { fontFace: SANS, bold: !!first, fontSize: pt(20), color: first ? WHITE : SOFT, fill: { color: ROW }, border, margin: [0.14, 0.17, 0.14, 0.17] } });
  s.addTable(
    [
      [th("Cadence"), th("Agent triage"), th("Script")],
      [td("Once, 25 groups", true), td("$0.07"), td("$0")],
      [td("1,000 runs of 25", true), td("about $74"), td("$0")],
      [td("Whole backlog, 27,981 groups", true), td("about $82"), td("$0")],
    ],
    { x: px(100), y: px(250), w: px(1720), colW: [px(860), px(430), px(430)], rowH: px(70), valign: "middle" }
  );
  caption(s, "Haiku 4.5 list price, measured on 25 groups and extrapolated. The Aiven AI gateway meters the exact figure per key.", { x: px(100), y: px(580), w: px(1720), h: px(36) });
  s.addNotes(NOTES[5]);

  // 7 ─ native chart ─────────────────────────────────────────────────────────
  s = pres.addSlide(); s.background = BG;
  eyebrow(s, "DEPLOYED ON AIVEN"); headline(s, "17K to 42K groups in 24 hours");
  const hours = ["11","12","13","14","15","16","17","18","19","20","21","22","23","00","01","02","03","04","05","06","07","08","09"];
  const vals = [470,933,970,916,952,1200,1172,1133,1138,1113,1153,1152,1160,1222,1279,1127,1033,1133,1180,1152,1145,1173,1120];
  s.addChart(pres.charts.BAR, [{ name: "New groups per hour", labels: hours, values: vals }], {
    x: px(100), y: px(226), w: px(1720), h: px(520), barDir: "col", barGapWidthPct: 25,
    chartColors: [GREEN], showLegend: false, showTitle: false,
    catAxisLabelColor: MUTED, valAxisLabelColor: MUTED, catAxisLabelFontFace: MONO, valAxisLabelFontFace: MONO,
    catAxisLabelFontSize: 12, valAxisLabelFontSize: 12,
    valGridLine: { color: DIV, size: 0.75 }, catGridLine: { style: "none" },
    valAxisMinVal: 0, valAxisMaxVal: 1400, valAxisMajorUnit: 400, valAxisLabelFormatCode: "#,##0",
  });
  caption(s, "New groups scraped per hour (UTC), 7 Oct 11:00 to 8 Oct 09:00. Steady at about 1,150 an hour.", { x: px(100), y: px(758), w: px(1720), h: px(36) });
  s.addNotes(NOTES[6]);

  // 8 ─ before / after ───────────────────────────────────────────────────────
  s = pres.addSlide(); s.background = BG;
  eyebrow(s, "THE RESULT"); headline(s, "Same search, 2.6x more groups");
  s.addImage({ path: `${DEMO}/out/before-search-3.png`, x: px(100), y: px(250), w: px(830), h: px(519) });
  s.addImage({ path: `${DEMO}/out/after-search-3.png`, x: px(990), y: px(250), w: px(830), h: px(519) });
  caption(s, [{ text: "Before: ", options: { bold: true, color: WHITE } }, { text: '15,442 groups. "data engineering": 10 results.' }], { x: px(100), y: px(789), w: px(830), h: px(60) });
  caption(s, [{ text: "After: ", options: { bold: true, color: GREEN } }, { text: '40,550 groups. "data engineering": 33 results.', options: { color: GREEN } }], { x: px(990), y: px(789), w: px(830), h: px(60) });
  s.addNotes(NOTES[7]);

  // 9 ─ list ─────────────────────────────────────────────────────────────────
  s = pres.addSlide(); s.background = BG;
  eyebrow(s, "THE HONEST BIT"); headline(s, "What we did not measure");
  const rows = [
    ["Quality of the agent's picks", "Unchecked. Treat the four priority 1 groups as examples, not a result."],
    ["Cost", "A 25-group sample at list price, extrapolated to the backlog."],
    ["Our first Luma fix", "It was wrong. We caught it by checking dates, not labels."],
  ];
  rows.forEach(([t, b], i) => {
    const y = 250 + i * 126;
    s.addShape(pres.ShapeType.rect, { x: px(100), y: px(y), w: px(1720), h: px(110), fill: { color: ROW }, line: { type: "none" } });
    s.addShape(pres.ShapeType.ellipse, { x: px(132), y: px(y + 34), w: px(10), h: px(10), fill: { color: GREEN }, line: { type: "none" } });
    body(s, t, { x: px(244), y: px(y + 24), w: px(1500), h: px(32), fontSize: pt(24), bold: true });
    s.addText(b, { x: px(244), y: px(y + 60), w: px(1500), h: px(28), fontFace: SANS, fontSize: pt(18), color: MUTED, isTextBox: true, margin: 0, valign: "top" });
  });
  body(s, "Measure cost + quality, or pay for retries.", { x: px(100), y: px(660), w: px(1720), h: px(44), fontSize: pt(30), bold: true });
  s.addNotes(NOTES[8]);

  await pres.writeFile({ fileName: OUT });
  console.log("wrote", OUT);
})();
