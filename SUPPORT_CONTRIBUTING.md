# Support and Contributions

This project is community-supported.

There is no formal support or service-level guarantee. The driver is provided
as-is and is intended for users comfortable with Hubitat drivers and virtual
devices.

## Getting Help

If something does not behave as expected:

1. Confirm your ICS URL is publicly accessible
2. Check the device attributes:

   * nextEvents
   * activeSummary
   * calendarTz

3. Enable debug logging and review rawDebug
4. Temporarily disable filters to isolate behavior

## Reporting Issues

Please use GitHub Issues:
https://github.com/truittchris/hubitat\_calendar\_switch\_ical/issues

When reporting an issue, include:

* Calendar provider ICS link
* Example event timing
* Relevant device attributes
* Redacted debug output if possible

## Contributions

Pull requests are welcome.

Guidelines:

* Keep changes focused and minimal
* Preserve existing behavior and defaults
* Avoid adding UI complexity
* Maintain hub-timezone-first behavior
* Keep the driver suitable for long-term maintenance

Significant feature requests should be discussed via an issue before
submitting a pull request.

