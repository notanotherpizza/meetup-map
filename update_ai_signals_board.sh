#!/bin/bash
# AI Signals - Fix board: delete duplicates, create missing #33
# Run: bash fix_ai_signals_board.sh

ORG="notanotherpizza"
PROJECT_NUMBER="1"

echo "Step 1: Deleting duplicate Backlog tickets..."
echo "(You'll need to find the item IDs from the board URL or via GraphQL)"
echo ""
echo "Quickest way to delete duplicates manually:"
echo "  1. Go to https://github.com/orgs/notanotherpizza/projects/1/views/1"
echo "  2. Open each of these Backlog items and use '...' > Delete item:"
echo "     - Meetup #30 - April 17th"
echo "     - Meetup #31 - May 13th (CIVO x Women in Data)"
echo "     - Meetup #32 x LangChain - June 9th"
echo "     - Meetup #33 - June 19th"
echo ""

echo "Step 2: Creating AI Signals #33..."

gh project item-create $PROJECT_NUMBER \
  --owner "$ORG" \
  --title "AI Signals #33 [19/06/2026] [TBC & TBC]" \
  --body "**Venue:** Brain Station
**Sponsor:** Brain Station / Daemon
**Capacity:** 80

**Talks:** TBC

---

### Action items

- [ ] 1 month + before the event: Book venue (for Civo tech junction email finley@civo.com)
- [ ] 1 month before event: Confirm speakers (see helpful templates https://www.notion.so/Email-Templates-227f116b6b0f80cda8c5d3a4857a79ea)
- [ ] 1 month before event: Schedule event on Meetup (copy previous event and update)
- [ ] 1 month before event: Schedule event on Luma (copy previous event and update)
- [ ] 1 month before event: Add event to website via Squarespace (copy previous event and update)
- [ ] 1 month before: Post event on LinkedIn
- [ ] 1 month before: Send a message about the event in Slack general
- [ ] 2 weeks before: Send out reminder via Meetup, Luma Newsletter, Slack general, and LinkedIn post
- [ ] Week before event: Confirm who will be hosting on the night and who will be recording, pick up AV kit if needed
- [ ] 48 hours before event: Create slide deck using the template https://docs.google.com/presentation/d/1tjsFMNhRRQpwLCq7iK2hKbAi1NI4Xx5SLh2rpPY5djQ/edit?usp=sharing
- [ ] 48 hours before event: Organise catering (for Daemon sponsored events reach out to Naomi Pearse on Slack)
- [ ] 24 hours before: Send out reminder via Meetup, Luma Newsletter, Slack general, and LinkedIn post
- [ ] At the event: Make sure all speakers have arrived and their laptop works with the display / their slides are added to the main deck
- [ ] At the event: Get pictures of speakers
- [ ] Day after the event: Post via Meetup, Luma Newsletter, and LinkedIn post thanking speakers and people for coming
- [ ] Week after event: Upload edited recording to YouTube channel and event on the website"

echo "Created: AI Signals #33"
echo ""
echo "Done! View the board at:"
echo "https://github.com/orgs/notanotherpizza/projects/1/views/1"
