const pptxgen = require("pptxgenjs");
const { applyTheme } = require("/Users/hugh/Library/Application Support/Claude/local-agent-mode-sessions/skills-plugin/b1649cdc-7b5f-4edf-92ac-7b688a899348/185a6ff1-e671-4193-8139-7be127d3ff4c/skills/pptx/scripts/apply_theme.js");

const REPO = "/Users/hugh/repos/meetup-map/demo";
const OUT = `${REPO}/slides/ato-case-study.pptx`;

const THEME = {
  name: "Aiven case study (dark)",
  headFontFace: "Calibri",
  bodyFontFace: "Calibri",
  colors: {
    dk1: "16171A", lt1: "F4F4F4", dk2: "1F2021", lt2: "B5B5B7",
    accent1: "1BB2B0", accent2: "FFC21A", accent3: "6F64FF",
    accent4: "21D16B", accent5: "C224D5", accent6: "444547",
    hlink: "2ED0CD", folHlink: "B5B5B7",
  },
};

const pres = new pptxgen();
pres.layout = "LAYOUT_16x9"; // 10 x 5.625 in
pres.title = "Case study: Not Another Pizza";
pres.theme = { headFontFace: THEME.headFontFace, bodyFontFace: THEME.bodyFontFace };
const C = pres.SchemeColor;

pres.defineSlideMaster({
  title: "CONTENT",
  background: { color: C.text1 },
  objects: [],
  slideNumber: { x: 9.2, y: 5.2, w: 0.5, h: 0.3, fontSize: 10, color: C.accent6 },
});
pres.defineSlideMaster({
  title: "CONTENT_TITLE",
  background: { color: C.text1 },
  objects: [
    {
      placeholder: {
        options: { name: "title", type: "title", x: 0.5, y: 0.3, w: 9, h: 0.9, fontSize: 30, bold: true, color: C.background1, valign: "top", margin: 0 },
        text: "",
      },
    },
  ],
  slideNumber: { x: 9.2, y: 5.2, w: 0.5, h: 0.3, fontSize: 10, color: C.accent6 },
});

const eyebrow = (s, t) =>
  s.addText(t, { x: 0.5, y: 0.2, w: 9, h: 0.3, fontSize: 12, bold: true, color: C.accent1, charSpacing: 3, isTextBox: true, margin: 0 });
const caption = (s, t, o = {}) =>
  s.addText(t, { x: 0.5, y: 4.85, w: 9, h: 0.5, fontSize: 14, color: C.accent2 === undefined ? C.background2 : C.background2, isTextBox: true, margin: 0, ...o });
const fs = require("fs");
const dataUri = (f) => "data:image/png;base64," + fs.readFileSync(f).toString("base64");
const poster = (s, name, mp4) =>
  s.addMedia({ type: "video", path: `${REPO}/out/${mp4}`, cover: dataUri(`${REPO}/slides/poster-${name}.png`), x: 1.5, y: 1.35, w: 7, h: 3.1 });

// 1 ─ section opener ───────────────────────────────────────────────────────
pres.addSection({ title: "Case study: Not Another Pizza" });
let s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Case study: Not Another Pizza" });
eyebrow(s, "CASE STUDY");
s.addText("Not Another Pizza: a script for the boring parts, an agent for the judgement", {
  x: 0.5, y: 1.3, w: 8.6, h: 1.9, fontSize: 34, bold: true, color: C.background1, isTextBox: true, margin: 0, valign: "top",
});
s.addText("A real index of 43,000 community meetup groups, and what it took to fix it", {
  x: 0.5, y: 3.5, w: 8.6, h: 0.8, fontSize: 20, color: C.background2, isTextBox: true, margin: 0, valign: "top",
});
s.addNotes(
`WHERE THIS FITS: insert as a block of nine slides straight after your "Script" slide (slide 9), before "Why not start with a deployable system" (slide 10). It is one worked example of everything slides 5-11 say in the abstract: prompt -> skill -> script -> deployable system.

THE POINT IN ONE LINE: we used a script wherever the answer was knowable, and an agent only for the one step that needed judgement, and we measured what that cost.

BACKGROUND: search.notanother.pizza (the project you credit on slide 13) indexes community meetup groups from Meetup and Luma. Hugh Evans built it. We found that most of the groups it knew about had never actually been scraped.

TIMING: about 1.5 minutes per slide, so roughly 12-14 minutes for the block. If you are short on time, cut slides 3 and 5 (the two short script videos) and keep 2, 4, 6 and 8.

IF EMBEDDED VIDEO DOES NOT SURVIVE THE IMPORT into Google Slides: upload the three MP4s (01_backlog, 02_agent_triage, 03_queue) to Drive and use Insert > Video > Google Drive on the slide.`);

// 2 ─ the problem ───────────────────────────────────────────────────────────
s = pres.addSlide({ masterName: "CONTENT_TITLE", sectionTitle: "Case study: Not Another Pizza" });
s.addText("64% of the index was missing", { placeholder: "title" });
s.addText("64%", { x: 0.5, y: 1.4, w: 4.2, h: 1.5, fontSize: 96, bold: true, color: C.accent1, isTextBox: true, margin: 0, valign: "middle" });
s.addText("27,981 of 43,425 listed groups had never been scraped, so searches could not find them.", {
  x: 0.5, y: 3.0, w: 4.2, h: 1.3, fontSize: 18, color: C.background1, isTextBox: true, margin: 0, valign: "top",
});
s.addImage({ path: `${REPO}/out/before-search-1.png`, x: 5.0, y: 1.45, w: 4.5, h: 2.81, shadow: { type: "outer", color: "000000", opacity: 0.4, blur: 8, offset: 3, angle: 90 } });
s.addText("Before: 15,442 groups indexed. 'kafka' found 13.", { x: 5.0, y: 4.4, w: 4.5, h: 0.5, fontSize: 14, color: C.background2, isTextBox: true, margin: 0 });
s.addNotes(
`WHERE THIS FITS: this is your "You need data" idea (slide 12) made concrete, so it can also sit directly before slide 12 if you prefer to tell it in two parts.

WHAT TO SAY: the index lists 43,425 groups, but only 15,442 were actually in it. The rest were found by discovery but never scraped. Nobody noticed because every search still returned something.

WHY IT MATTERS FOR YOUR TALK: this is the "you don't know what you're missing" point from your Continuous Iteration Trap slide (slide 4). Context is defined by data, and here the data was silently incomplete. A model sitting on top of this would have confidently answered from 36% of the picture.

THE SCREENSHOT is the live site before the fix: 15,442 groups and 13 results for "kafka".

CAUTION: this number is as of 7 October 2026. The index has since grown, which slides 7 and 8 show.`);

// 3 ─ script finds the gap ──────────────────────────────────────────────────
s = pres.addSlide({ masterName: "CONTENT_TITLE", sectionTitle: "Case study: Not Another Pizza" });
s.addText("A script found the gap: 0.1s, 0 tokens", { placeholder: "title" });
poster(s, "01_backlog", "01_backlog.mp4");
caption(s, "Compare the group list with the index. No model needed.");
s.addNotes(
`WHERE THIS FITS: this is your "Script" slide (slide 9) with a real example, and it answers your slide 8 question, "Is this a problem that AI needs to architect on a daily?" No.

VIDEO (3 seconds, silent): 01_backlog.mp4. It prints: 43,425 listed, 15,442 in the index, 27,981 never scraped (64%), tokens used 0, wall clock 0.11s.

WHAT TO SAY: "Yesterday's solution is not always today's best solution. We could have asked a model 'which groups are missing?' and paid for it every time. A set difference between two lists answers it in a tenth of a second, for free, every time, and it is the same answer on the thousandth run."

LINK TO YOUR ARC: prompt -> skill -> script. This is the script end. The cost of this step does not grow with how often you run it, which matters when you show the cost-by-cadence table on slide 6 of this block.

IF THE VIDEO DOES NOT PLAY: it is a 3-second terminal recording; read the four numbers out instead.`);

// 4 ─ agent for judgement ───────────────────────────────────────────────────
s = pres.addSlide({ masterName: "CONTENT_TITLE", sectionTitle: "Case study: Not Another Pizza" });
s.addText("Where an agent earns its keep", { placeholder: "title" });
poster(s, "02_agent_triage", "02_agent_triage.mp4");
caption(s, "25 groups triaged for $0.07. Two tools, one job, read-only.");
s.addNotes(
`WHERE THIS FITS: after "Script", as the one place an agent is justified. It also lands your slide 6 and 7 contrast (prompt engineering vs skill engineering): this agent has a narrow brief and two tools, not an open-ended prompt.

VIDEO (53 seconds, silent): 02_agent_triage.mp4. For each of 25 never-scraped groups the agent looks at the group's public page and labels it priority 1 (active tech/developer/data community), 2 (normal) or 3 (hobby/social/dormant), with a one-line reason. A running cost counter ticks up on the right. It finishes at 45,860 input and 5,522 output tokens, $0.0735, about $0.003 a group.

WHAT TO SAY: "This is the only step that needs judgement: is this group worth scraping before the others? Everything around it is a script. The agent can't scrape, write to the database or change the queue. It only recommends."

LINK TO YOUR SLIDES 3-4: this is how you know what you're spending. The counter is the point, not the labels.

RESULT: 4 of 25 came out priority 1 (an AI community, a React Native community, a SharePoint developer group, a tech meetup), 4 priority 2, 17 priority 3.

IMPORTANT, SAY THIS OUT LOUD: we measured cost, not quality. Nobody has checked whether those four picks are right. Treat them as examples, not as a result. (The last slide of this block says it again.)

MODEL AND GATEWAY: Claude Haiku 4.5, called through the Aiven AI gateway so usage is metered per key.`);

// 5 ─ script acts ───────────────────────────────────────────────────────────
s = pres.addSlide({ masterName: "CONTENT_TITLE", sectionTitle: "Case study: Not Another Pizza" });
s.addText("Then a script acts on the answer", { placeholder: "title" });
poster(s, "03_queue", "03_queue.mp4");
caption(s, "The agent decides. Code does the work. 4 of 25 moved to the front.");
s.addNotes(
`WHERE THIS FITS: still on your "Script" slide (slide 9). It shows the boundary between the agent and the system around it.

VIDEO (3 seconds, silent): 03_queue.mp4. It reads the agent's output and moves the four priority-1 groups to the front of the 27,981-group queue.

WHAT TO SAY: "The agent returned a list. A script, not the agent, turned that list into a queue. That is deliberate: the part that has to be reliable is deterministic code, and the part that needs a human-like judgement is the model."

LINK TO YOUR GOVERNANCE SLIDES (22-26): Access means "enough to do the exact job and no more". The agent has two tools and no write access. If it is wrong, the worst case is a group scraped a little later.

CAN BE CUT if you are short of time. Slide 3's video already makes the script point.`);

// 6 ─ cost by cadence ───────────────────────────────────────────────────────
s = pres.addSlide({ masterName: "CONTENT_TITLE", sectionTitle: "Case study: Not Another Pizza" });
s.addText("What it costs, by how often you ask", { placeholder: "title" });
const hdr = (t) => ({ text: t, options: { bold: true, color: C.background1, fill: { color: C.accent6 }, fontSize: 16 } });
const cell = (t, o = {}) => ({ text: t, options: { color: C.background1, fontSize: 16, ...o } });
s.addTable(
  [
    [hdr("Cadence"), hdr("Agent triage"), hdr("Script")],
    [cell("Once, 25 groups"), cell("$0.07"), cell("$0")],
    [cell("1,000 runs of 25"), cell("about $74"), cell("$0")],
    [cell("Whole backlog, 27,981 groups"), cell("about $82"), cell("$0")],
  ],
  { x: 0.5, y: 1.5, w: 9, colW: [4.4, 2.3, 2.3], rowH: 0.55, border: { type: "solid", pt: 1, color: C.accent6 }, valign: "middle" }
);
s.addText("Haiku 4.5 list price, measured on 25 groups and extrapolated. The Aiven gateway meters the exact figure per key.", {
  x: 0.5, y: 4.2, w: 9, h: 0.7, fontSize: 14, color: C.background2, isTextBox: true, margin: 0, valign: "top",
});
s.addNotes(
`WHERE THIS FITS: this is the "Cost? Run once, run weekly, 1000 runs" prompt on your Continuous Iteration Trap slide (slide 4). Put it right after or in place of that slide's placeholder.

THE NUMBERS: measured on a real run of 25 groups, 45,860 input and 5,522 output tokens, $0.0735 at Claude Haiku 4.5 list price. $0.0735 / 25 = $0.00294 a group. 1,000 runs of 25 groups = about $74. Triaging all 27,981 groups once = about $82.

WHAT TO SAY: "The script costs nothing at any cadence. The agent's cost scales with how often you ask it to think. So the question is not 'agent or script' but 'which step actually needs thinking', and how often do you run it."

CAVEATS: this is list price and a 25-group sample, extrapolated. The Aiven AI gateway shows the real metered cost per access key; if that differs, use the console figure.

LINK TO YOUR SLIDE 8: "AI will burn your token budget down". This is the counter-example: a bounded agent with a known cost.`);

// 7 ─ deployed & backfilled ─────────────────────────────────────────────────
s = pres.addSlide({ masterName: "CONTENT_TITLE", sectionTitle: "Case study: Not Another Pizza" });
s.addText("17k to 42k groups in 24 hours", { placeholder: "title" });
const hours = ["11","12","13","14","15","16","17","18","19","20","21","22","23","00","01","02","03","04","05","06","07","08","09"];
const vals = [470,933,970,916,952,1200,1172,1133,1138,1113,1153,1152,1160,1222,1279,1127,1033,1133,1180,1152,1145,1173,1120];
s.addChart(pres.charts.BAR, [{ name: "New groups per hour", labels: hours, values: vals }], {
  x: 0.5, y: 1.4, w: 9, h: 3.3, barDir: "col",
  chartColors: [THEME.colors.accent1],
  catAxisLabelColor: THEME.colors.lt2, valAxisLabelColor: THEME.colors.lt2,
  catAxisLabelFontFace: "+mn-lt", valAxisLabelFontFace: "+mn-lt", catAxisLabelFontSize: 12, valAxisLabelFontSize: 12,
  valGridLine: { color: THEME.colors.accent6, size: 0.5 }, catGridLine: { style: "none" },
  showLegend: false, showTitle: false, valAxisMaxVal: 1400, valAxisMajorUnit: 400,
  showCatAxisTitle: true, catAxisTitle: "hour (UTC), 7 Oct 11:00 to 8 Oct 09:00", catAxisTitleColor: THEME.colors.lt2, catAxisTitleFontSize: 12,
});
s.addText("New groups scraped per hour after the fix: steady at about 1,150.", { x: 0.5, y: 4.8, w: 9, h: 0.4, fontSize: 14, color: C.background2, isTextBox: true, margin: 0 });
s.addNotes(
`WHERE THIS FITS: your "Why not start with a deployable system instead?" and "Deployable System" slides (10 and 11), and the "Aiven Runtimes" idea. Put it directly after slide 11.

THE STORY: the scraper runs as an Aiven App writing to Aiven Postgres. It never reached the missing groups: the app was building from a stale branch whose group list predated about 26,000 later-discovered groups, and it worked through that list in file order. The fix was to order by what is already in the database (never-scraped first, then stalest) and to ship the full list. Once deployed it ran at a steady 1,150 groups an hour: the database went from about 17,000 to 42,068 groups in 24 hours.

WHAT TO SAY: "The thing that fixed it was boring: a script that reads state from the database instead of from a file in the container. That is what a deployable system gives you: state that survives restarts."

TO ADD BY HAND (not in this file): a screenshot of the Aiven console showing the batch app's logs with '[n/43419]' on slide 10 or 11. It proves the deployment and the scale.

ACCURACY: the first bar (11:00) is partial because the scraper started around 11:30. The figures are from the production database on 8 October.`);

// 8 ─ before / after ────────────────────────────────────────────────────────
s = pres.addSlide({ masterName: "CONTENT_TITLE", sectionTitle: "Case study: Not Another Pizza" });
s.addText("Same search, 2.6x more groups", { placeholder: "title" });
s.addImage({ path: `${REPO}/out/before-search-3.png`, x: 0.5, y: 1.5, w: 4.3, h: 2.69, shadow: { type: "outer", color: "000000", opacity: 0.4, blur: 8, offset: 3, angle: 90 } });
s.addImage({ path: `${REPO}/out/after-search-3.png`, x: 5.2, y: 1.5, w: 4.3, h: 2.69, shadow: { type: "outer", color: "000000", opacity: 0.4, blur: 8, offset: 3, angle: 90 } });
s.addText("Before: 15,442 groups. 'data engineering': 10 results", { x: 0.5, y: 4.3, w: 4.3, h: 0.7, fontSize: 14, color: C.background2, isTextBox: true, margin: 0, valign: "top" });
s.addText("After: 40,550 groups. 'data engineering': 33 results", { x: 5.2, y: 4.3, w: 4.3, h: 0.7, fontSize: 14, color: C.accent1, bold: true, isTextBox: true, margin: 0, valign: "top" });
s.addNotes(
`WHERE THIS FITS: slide 13, your shout-out to notanother.pizza and Hugh Evans. This slide is the payoff for that credit.

THE NUMBERS (live site, before 6-7 October, after 8 October): groups 15,442 -> 40,550; events 829,236 -> 2,029,657. The search "data engineering" went from 10 to 33 results; "kafka" from 13 to 20. The site's displayed counts differ slightly from the database (43,414 groups) because it is rendered once a day.

HONEST NOTE: not every search grew. "python AND london" returned 1 result both before and after. More data does not mean more of every answer.

WHAT TO SAY: "Same site, same query, same code. The only change was the data behind it. That is your 'you need data' point, and it is why the governance slides matter: structure, visibility, access and context only work if the data is actually there."

SHOUT-OUT: Hugh Evans built and maintains this project; search.notanother.pizza.`);

// 9 ─ what we did not measure ───────────────────────────────────────────────
s = pres.addSlide({ masterName: "CONTENT_TITLE", sectionTitle: "Case study: Not Another Pizza" });
s.addText("What we did not measure", { placeholder: "title" });
s.addText(
  [
    { text: "Quality of the agent's picks: unchecked", options: { bullet: true, breakLine: true } },
    { text: "Cost: a 25-group sample at list price, extrapolated", options: { bullet: true, breakLine: true } },
    { text: "Our first fix for the Luma scraper was wrong. We found out by checking dates, not labels", options: { bullet: true } },
  ],
  { x: 0.5, y: 1.5, w: 9, h: 2.8, fontSize: 22, color: C.background1, isTextBox: true, margin: 0, valign: "top", paraSpaceAfter: 14 }
);
s.addText("Measure cost and quality, or you will pay for retries.", { x: 0.5, y: 4.5, w: 9, h: 0.5, fontSize: 18, bold: true, color: C.accent2, isTextBox: true, margin: 0 });
s.addNotes(
`WHERE THIS FITS: close the block with this, then continue to your "How can companies leverage AI post-hype?" slide (14). It is also the honest version of your slide 3 ("Are you actually seeing ROI?").

THE THREE THINGS:
1. We measured what the agent cost, not whether its priority-1 picks are right. A proper check would compare its labels against a human-labelled sample.
2. The cost figures are a 25-group run at Claude Haiku 4.5 list price, extrapolated to the full backlog. Use the Aiven gateway's per-key figure for the exact number.
3. We made a mistake and caught it. The first Luma fix read 'past events' from a page that actually returns upcoming events, so it would have labelled future events as past. The label looked right; the dates were in the future. The corrected version uses Luma's own calendar API and refuses to label an event past unless its start is in the past. 746 of 765 Luma groups now have real past events, 37,233 in total, and none has a future date.

WHAT TO SAY: "Cost and quality both have to be measured. Cutting cost without measuring quality just buys you retries, and retries are the most expensive tokens."`);

(async () => {
  await pres.writeFile({ fileName: OUT });
  await applyTheme(OUT, THEME);
  console.log("wrote", OUT);
})();
