# HE iCal Switch

A Hubitat device driver that monitors a public iCal / ICS calendar feed and maps calendar activity to a virtual switch.

The switch turns ON when an eligible calendar event is active and OFF when no eligible event is active.

This driver is designed to be reliable with real-world calendars, and to always respect the hub’s configured timezone.

---

## Find this useful?  Tips are gladly accepted!

https://www.christruitt.com/tip-jar
Web: https://christruitt.com
Email: hello@christruitt.com

---

## What This Driver Is (and Is Not)

This driver is:
- A lightweight automation signal source
- A way to represent “busy time” as a switch
- Intended for Rule Machine, dashboards, and modes

This driver is not:
- A full calendar viewer
- A scheduling engine
- A replacement for native calendar apps

---

## Key Features

- Works with any public iCal / ICS URL - DOES NOT REQUIRE a Google developer or Microsoft Azure account
- Correct timezone handling using the hub’s timezone
- Handles:
  - UTC (Z) timestamps
  - Calendar-level timezone hints (X-WR-TIMEZONE, VTIMEZONE)
  - Floating timestamps
- Fine-grained event filtering:
  - Busy vs free
  - Tentative
  - Declined (when attendee data exists)
  - All-day events (optional)
- Keyword include / exclude filters
- Start and end offsets
- Displays upcoming eligible events for validation
- Schedules precise transitions at event boundaries

---

## Typical Uses

- “In a meeting” virtual switch
- Suppressing announcements during meetings
- Driving Rule Machine logic
- Mode or lighting changes when busy
- Dashboard indicators

---

## Requirements

- Hubitat Elevation hub
- A publicly accessible iCal / ICS URL

Private or authenticated calendar feeds are not supported.

---

## Installation

1. Go to Drivers Code
2. Click New Driver
3. Paste the driver code
4. Click Save
5. Create a Virtual Device
6. Select driver: HE iCal Switch

---

## Basic Configuration

Open the device and go to Preferences.

Required:
- ICS URL – Public calendar feed URL

Recommended defaults:
- Poll interval: 900 seconds
- Include events started within past N hours: 4–6
- Look ahead N days: 2–3

---

## Why the Timing Window Settings Exist

Include events started within past N hours:
This prevents missed events when polling intervals do not align exactly with event start times and helps tolerate feed delays.

Look ahead N days:
Limits how far into the future events are evaluated to improve performance and keep automation focused on near-term events.

---

## Event Filters

- Trigger only for Busy events (TRANSP != TRANSPARENT)
- Exclude tentative events
- Exclude declined events when attendee data is present
- Trigger for all-day events (optional)

---

## Keyword Filters

Comma-separated lists matched against event summary and location.

---

## Attributes Exposed

- switch
- active
- activeSummary
- nextSummary
- nextEvents
- calendarTz
- lastFetch
- lastStatus
- rawDebug

---

## Commands

- Initialize
- Poll
- Show Next
- Clear Debug

---

## Minimal Troubleshooting

Switch never turns ON:
- Confirm the ICS URL is public
- Click Poll manually
- Use Show Next to verify events are detected

Times look wrong:
- Verify the hub timezone
- Check the calendarTz attribute
- Ensure the ICS URL is not cached or redirected

Events missing:
- Temporarily disable filters
- Increase look-back or look-ahead values
- Review keyword filters

---

## Known Limitations

- Requires a public ICS URL
- Read-only; cannot create or modify events
- Dependent on how providers expand recurring events
- Some calendars omit TRANSP or STATUS fields
- Not intended as a calendar UI

---

## Author

Chris Truitt  
GitHub: https://github.com/truittchris/hubitat_calendar_switch_ical
Contact: hello@christruitt.com  
Web: https://christruitt.com

Find this useful?  Tips are gladly accepted!

Buy Me a Coffee: https://buymeacoffee.com/christruitt
PayPal: https://www.paypal.com/paypalme/ctruit01/5
Venmo: https://venmo.com/christruitt

---

## License

Free for public use.

You may use, modify, and redistribute this driver provided attribution remains intact.

Provided “as is” with no warranty.
