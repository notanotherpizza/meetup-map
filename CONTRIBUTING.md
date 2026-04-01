# Contributing to MeetupMap

## Add your group to the map

MeetupMap scrapes Meetup Pro Networks daily and renders a global map of community groups. You can add any group directly — even if it's not part of a Pro Network.

### How to add your group

1. Open [`community/groups.txt`](community/groups.txt) in this repo
2. Click the ✏️ edit button (top right of the file view)
3. Add your group's URL on a new line:
   ```
   https://www.meetup.com/your-group-name/
   ```
4. Scroll down and click **Propose changes**
5. Submit the pull request

That's it. No need to clone the repo or install anything. Your group will appear on the map after the next daily scrape (runs at 06:00 UTC).

### Supported platforms

| Platform | Status |
|----------|--------|
| [Meetup.com](https://meetup.com) | ✅ Supported |
| [Luma](https://lu.ma) | 🔜 Coming soon |

### What data is collected?

- Group name, URL, city, country
- Member count
- Past and upcoming events (venue, date, RSVP count)

No personal data about members is collected.

### Opt out

To remove your group from the map, open a PR removing your URL from `community/groups.txt`, or open an issue and we'll remove it for you.