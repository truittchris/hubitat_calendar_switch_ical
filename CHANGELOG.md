# Changelog


## [1.2.1] - 2026-02-04

### Fixed
- **Outlook / Microsoft 365 timezones**: Maps Windows TZIDs (for example `Eastern Standard Time`) to IANA zones so event times don’t silently fall back to GMT.
- Resolved an issue where some recurring events (including bi-weekly meetings) could appear “missing” because DTSTART/DTEND were shifted by an incorrect timezone.

## [1.2.0] - 2026-01-31

### Added
- **Monthly Recurrence Support**: The driver now processes `FREQ=MONTHLY` rules.
- **Relative Date Logic (BYDAY)**: Added support for monthly events defined by relative positions, such as:
  - Specific instances: "2nd Monday of the month" (`2MO`)
  - End-of-month instances: "Last Friday of the month" (`-1FR`)
- **Monthly Interval Support**: Correctly handles intervals for monthly events (e.g., "Every 3 months").

### Fixed
- Fixed an issue where "Monthly Solution Advisory" and "Performance Goal" meetings from Outlook were being skipped with an `unsupported FREQ` error.
- Improved the `RRULE` expansion loop to ensure monthly instances are generated within the configured `horizonDays` window.
- Restored and optimized the `debugAdd` buffer logic to ensure character limits are respected while logging recurring event expansion.

## 1.0.0 – 2026-01-07

* Initial public release of Hubitat Calendar Switch iCal
* Monitors an iCal/ICS feed and maps eligible calendar activity to a virtual switch
* Uses hub timezone for display and scheduling (no timezone override required)
* Supports Outlook / Microsoft 365 timezone parsing (X-WR-TIMEZONE and VTIMEZONE)
* Expands common weekly RRULE patterns and applies RECURRENCE-ID overrides
* Transition-based polling at event boundaries plus regular polling cadence
* Keyword include/exclude filters and event eligibility controls (busy-only, tentative, declined, all-day)
