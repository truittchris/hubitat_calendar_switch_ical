# Changelog

## 1.0.0 – 2026-01-07

* Initial public release of Hubitat Calendar Switch iCal
* Monitors an iCal/ICS feed and maps eligible calendar activity to a virtual switch
* Uses hub timezone for display and scheduling (no timezone override required)
* Supports Outlook / Microsoft 365 timezone parsing (X-WR-TIMEZONE and VTIMEZONE)
* Expands common weekly RRULE patterns and applies RECURRENCE-ID overrides
* Transition-based polling at event boundaries plus regular polling cadence
* Keyword include/exclude filters and event eligibility controls (busy-only, tentative, declined, all-day)
