/**
 * HE iCal Switch - Patched for Weekly and Advanced Monthly Support
 *
 * Author: Chris Truitt
 * GitHub:  https://github.com/truittchris/he_ical_switch
 * Contact: hello@christruitt.com
 *
 * Summary
 * - Follows an iCal (ICS) URL and drives a Hubitat switch based on eligible events
 * - Supports Outlook / Microsoft 365 calendar feeds
 * - Patched: Supports WEEKLY and MONTHLY (including BYDAY rules like "2nd Tuesday")
 */

import java.text.SimpleDateFormat
import java.util.Calendar
import groovy.transform.Field

@Field static final String DRIVER_VERSION = "2026.02.04.2"

@Field static final Map<String,String> WINDOWS_TZ_MAP = [
    "Pacific Standard Time":"America/Los_Angeles",
    "Mountain Standard Time":"America/Denver",
    "Central Standard Time":"America/Chicago",
    "Eastern Standard Time":"America/New_York",
    "US Eastern Standard Time":"America/New_York",
    "Atlantic Standard Time":"America/Halifax",
    "Alaskan Standard Time":"America/Anchorage",
    "Hawaiian Standard Time":"Pacific/Honolulu",
    "UTC":"UTC",
    "Greenwich Standard Time":"Etc/GMT"
]

private TimeZone resolveTimeZone(String tzid, TimeZone fallback) {
    if (!tzid) return fallback
    tzid = tzid.replace('"','').trim()
    // Some Outlook feeds use "tzone://Microsoft/Custom" style identifiers – try the tail token.
    if (tzid.toLowerCase().startsWith("tzone://")) {
        String tail = tzid.tokenize("/").last()
        if (tail) tzid = tail
    }
    TimeZone tz = TimeZone.getTimeZone(tzid)
    boolean looksUnknown = (tz?.getID() == "GMT") && !(tzid.equalsIgnoreCase("GMT") || tzid.equalsIgnoreCase("UTC") || tzid.toUpperCase().startsWith("ETC/") || tzid.toUpperCase().contains("GMT"))
    if (looksUnknown) {
        String mapped = WINDOWS_TZ_MAP[tzid] ?: WINDOWS_TZ_MAP[tzid.trim()]
        if (mapped) return TimeZone.getTimeZone(mapped)
        return fallback
    }
    return tz ?: fallback
}


metadata {
    definition(name: "Hubitat Calendar Switch iCal", namespace: "truittchris", author: "Chris Truitt") {
        capability "Switch"
        capability "Refresh"

        command "initialize"
        command "poll"
        command "pollRegular"
        command "pollTransition"
        command "clearDebug"
        command "showNext"

        attribute "active", "bool"
        attribute "activeSummary", "string"
        attribute "nextSummary", "string"
        attribute "nextEvents", "string"
        attribute "lastFetch", "string"
        attribute "lastStatus", "string"
        attribute "rawDebug", "string"
        attribute "calendarTz", "string"
        attribute "driverVersion", "string"
    }
}

preferences {
    input name: "icsUrl", type: "string", title: "ICS URL", required: true
    input name: "pollSeconds", type: "number", title: "Poll interval (seconds)", defaultValue: 900
    input name: "includePastHours", type: "number", title: "Include events started within past N hours", defaultValue: 6
    input name: "horizonDays", type: "number", title: "Look ahead N days", defaultValue: 3
    input name: "maxEvents", type: "number", title: "Max events to consider per poll", defaultValue: 80
    input name: "triggerBusyOnly", type: "bool", title: "Trigger only for Busy events (TRANSP != TRANSPARENT)", defaultValue: true
    input name: "excludeTentative", type: "bool", title: "Exclude tentative events (STATUS=TENTATIVE)", defaultValue: false
    input name: "excludeDeclinedIfPresent", type: "bool", title: "Exclude declined events when PARTSTAT is present", defaultValue: false
    input name: "triggerAllDay", type: "bool", title: "Trigger for all-day events", defaultValue: false
    input name: "includeKeywords", type: "string", title: "Include keywords (comma-separated)", required: false
    input name: "excludeKeywords", type: "string", title: "Exclude keywords (comma-separated)", required: false
    input name: "startOffsetMin", type: "number", title: "Start offset (minutes)", defaultValue: 0
    input name: "endOffsetMin", type: "number", title: "End offset (minutes)", defaultValue: 0
    input name: "nextListCount", type: "number", title: "Next events list size", defaultValue: 10
    input name: "nextListShowLocation", type: "bool", title: "Include location in next events list", defaultValue: true
    input name: "debugLogging", type: "bool", title: "Enable debug logging", defaultValue: true
    input name: "debugMaxChars", type: "number", title: "Max debug buffer chars", defaultValue: 6000
}

def installed() {
    sendEvent(name: "switch", value: "off")
    sendEvent(name: "active", value: false)
    initialize()
}

def updated() {
    initialize()
}

def initialize() {
    unschedule()
    state.lastPollMs = null
    state.nextTransitionAtMs = null
    state.calendarTzid = null
    sendEvent(name: "driverVersion", value: DRIVER_VERSION)
    sendEvent(name: "lastStatus", value: "Initializing")
    runIn(2, "pollTransition")
}

def refresh() { pollTransition() }
def showNext() { pollTransition() }

def clearDebug() {
    state.debugBuf = ""
    sendEvent(name: "rawDebug", value: "")
}

def on() { sendEvent(name: "switch", value: "on") }
def off() { sendEvent(name: "switch", value: "off") }

def pollRegular() { poll([force: false, reason: "regular"]) }
def pollTransition() { poll([force: true, reason: "transition"]) }

def poll(Map data = null) {
    boolean force = (data?.force == true)
    String reason = (data?.reason ?: "manual")

    if (!icsUrl?.trim()) {
        sendEvent(name: "lastStatus", value: "Missing ICS URL")
        scheduleNextPoll()
        return
    }

    Long nowMs = now()
    Long lastPoll = (state.lastPollMs instanceof Long) ? (state.lastPollMs as Long) : 0L
    if (!force && lastPoll && (nowMs - lastPoll) < 5000) {
        runIn(2, "pollRegular")
        return
    }
    state.lastPollMs = nowMs
    sendEvent(name: "lastStatus", value: "Fetching")

    String body = null
    Integer statusCode = null
    try {
        httpGet([uri: icsUrl, timeout: 25]) { resp ->
            statusCode = resp?.status
            if (resp?.data != null) {
                body = (resp.data instanceof String) ? (resp.data as String) : resp.data.getText("UTF-8")
            }
        }
    } catch (e) {
        debugAdd("Fetch exception: ${e}")
        sendEvent(name: "lastStatus", value: "Fetch exception")
        scheduleNextPoll()
        return
    }

    sendEvent(name: "lastFetch", value: fmtStamp(new Date(), hubTz()))
    if (!body || statusCode != 200) {
        sendEvent(name: "lastStatus", value: "Fetch failed")
        scheduleNextPoll()
        return
    }

    TimeZone calTz = detectCalendarTimeZone(body)
    debugAdd("Detected calendar TZ: ${calTz?.getID()}")
    state.calendarTzid = calTz?.getID()
    sendEvent(name: "calendarTz", value: state.calendarTzid)

    List<Map> events = parseIcs(body, calTz)
    debugAdd("Parsed events: ${events?.size()}")
    events?.take(5)?.each { ev -> debugAdd("Event: ${ev.summary} start=${new Date(ev.startMs)} end=${new Date(ev.endMs)} tz=${ev.recTzId} allDay=${ev.allDay} rrule=${ev.rrule ?: ""}") }
    Long windowStart = nowMs - (safeInt(includePastHours, 6) * 3600000L)
    Long windowEnd   = nowMs + (safeInt(horizonDays, 3) * 86400000L)

    events = expandRecurringEvents(events, windowStart, windowEnd, calTz)

    Long startOffsetMs = safeInt(startOffsetMin, 0) * 60000L
    Long endOffsetMs   = safeInt(endOffsetMin, 0) * 60000L

    List<Map> eligible = events.findAll { ev ->
        Long s = (ev?.startMs as Long)
        Long e = (ev?.endMs as Long)
        if (!s || !e) return false
        Long es = s + startOffsetMs
        Long ee = e + endOffsetMs
        (ee >= windowStart) && (es <= windowEnd) && isEligible(ev)
    }.collect { ev ->
        ev.effStartMs = (ev.startMs as Long) + startOffsetMs
        ev.effEndMs   = (ev.endMs as Long) + endOffsetMs
        return ev
    }.sort { a, b -> (a.effStartMs as Long) <=> (b.effStartMs as Long) }

    Integer cap = safeInt(maxEvents, 80)
    if (eligible.size() > cap) eligible = eligible.take(cap)

    List<Map> activeNow = eligible.findAll { it.effStartMs <= nowMs && nowMs < it.effEndMs }
    Map next = eligible.find { it.effStartMs > nowMs }
    
    sendEvent(name: "active", value: (activeNow.size() > 0))
    sendEvent(name: "activeSummary", value: activeNow ? formatEventLine(activeNow[0], hubTz()) : null)
    sendEvent(name: "nextSummary", value: next ? formatEventLine(next, hubTz()) : null)

    String nextEventsText = eligible.findAll { it.effEndMs >= nowMs }.take(safeInt(nextListCount, 10)).collect { 
        "• " + formatEventLineForList(it, hubTz(), (nextListShowLocation == true)) 
    }.join("\n")
    sendEvent(name: "nextEvents", value: nextEventsText)

    String desiredSwitch = activeNow ? "on" : "off"
    if (desiredSwitch != device.currentValue("switch")) sendEvent(name: "switch", value: desiredSwitch)

    scheduleNextTransition(activeNow, next, nowMs)
    sendEvent(name: "lastStatus", value: "OK")
    scheduleNextPoll()
}

private List<Map> expandRecurringEvents(List<Map> events, Long windowStart, Long windowEnd, TimeZone calendarTz) {
    Map<String, Map> overrides = [:]
    events.findAll { it.recurrenceIdMs != null }.each { overrides["${it.uid}@@${it.recurrenceIdMs}"] = it }
    
    List<Map> out = []
    events.each { ev -> if (!ev.rrule || ev.recurrenceIdMs) out << ev }
    events.findAll { it.rrule && !it.recurrenceIdMs }.each { master ->
        out.addAll(expandMaster(master, windowStart, windowEnd, calendarTz, overrides))
    }
    return out
}

private List<Map> expandMaster(Map master, Long windowStart, Long windowEnd, TimeZone calendarTz, Map overrides) {
    List<Map> out = []
    Map rr = parseRrule(master.rrule)
    String freq = rr?.FREQ?.toUpperCase()

    TimeZone tz = TimeZone.getTimeZone(master.recTzId) ?: calendarTz ?: hubTz()
    Calendar startCal = Calendar.getInstance(tz)
    startCal.setTimeInMillis(master.startMs)

    Long untilMs = rr.UNTIL ? parseUntilMs(rr.UNTIL, tz, calendarTz) : Long.MAX_VALUE
    Integer interval = safeInt(rr.INTERVAL, 1)

    // If we don't recognize the frequency, keep the master as a single instance (otherwise it disappears).
    if (freq != "WEEKLY" && freq != "MONTHLY") {
        debugAdd("RRULE unsupported FREQ='${freq}' summary='${master.summary}' – treating as non-recurring")
        if (master.startMs <= windowEnd && master.endMs >= windowStart) out << master
        return out
    }

    Calendar curr = Calendar.getInstance(tz)
    curr.setTimeInMillis(windowStart)
    curr.set(Calendar.HOUR_OF_DAY, startCal.get(Calendar.HOUR_OF_DAY))
    curr.set(Calendar.MINUTE, startCal.get(Calendar.MINUTE))
    curr.set(Calendar.SECOND, startCal.get(Calendar.SECOND))
    curr.set(Calendar.MILLISECOND, 0)

    while (curr.timeInMillis <= windowEnd && curr.timeInMillis <= untilMs) {
        if (curr.timeInMillis >= master.startMs) {
            boolean match = false

            if (freq == "WEEKLY") {
                // RFC5545: WEEKLY interval counts weeks from DTSTART; BYDAY may include multiple days.
                List<String> byday = rr.BYDAY ? rr.BYDAY.split(",")*.trim() : [dowToByday(startCal.get(Calendar.DAY_OF_WEEK))]
                String currDow = dowToByday(curr.get(Calendar.DAY_OF_WEEK))
                if (byday.contains(currDow)) {
                    long days = epochDay(curr.timeInMillis, tz) - epochDay(master.startMs, tz)
                    long weeks = (long) Math.floor(days / 7.0d)
                    if (weeks % interval == 0) match = true
                }
            } else if (freq == "MONTHLY") {
                int months = (curr.get(Calendar.YEAR) - startCal.get(Calendar.YEAR)) * 12 + (curr.get(Calendar.MONTH) - startCal.get(Calendar.MONTH))
                if (months % interval == 0) {
                    match = matchesMonthly(curr, startCal, rr)
                }
            }

            if (match) {
                String key = "${master.uid}@@${curr.timeInMillis}"
                if (overrides[key]) {
                    if (overrides[key].status != "CANCELLED") out << overrides[key]
                } else {
                    Map gen = [:] + master
                    gen.startMs = curr.timeInMillis
                    gen.endMs = curr.timeInMillis + master.durationMs
                    gen.rrule = null
                    out << gen
                }
            }
        }
        curr.add(Calendar.DATE, 1)
    }
    return out
}

private boolean checkMonthlyByDay(Calendar curr, String byDayRule) {
    String dayPart = byDayRule.replaceAll("[^A-Z]", "")
    String numPart = byDayRule.replaceAll("[A-Z]", "")
    if (dowToByday(curr.get(Calendar.DAY_OF_WEEK)) != dayPart) return false
    if (!numPart) return true

    int occurrence = numPart.toInteger()
    if (occurrence > 0) {
        return ((curr.get(Calendar.DAY_OF_MONTH) - 1) / 7).toInteger() + 1 == occurrence
    } else {
        Calendar nextWeek = (Calendar)curr.clone()
        nextWeek.add(Calendar.DATE, 7)
        return nextWeek.get(Calendar.MONTH) != curr.get(Calendar.MONTH)
    }
}

private TimeZone detectCalendarTimeZone(String icsText) {
    List<String> lines = unfoldLines(icsText)
    String xwr = lines.find { it.startsWith("X-WR-TIMEZONE:") }
    if (xwr) {
        String tzid = xwr.substring(15).trim().replace("\"", "")
        TimeZone tz = resolveTimeZone(tzid, null)
        if (tz) return tz
    }
    // Fallback: pick the first VTIMEZONE TZID if present (common in Outlook feeds).
    String vtz = lines.find { it.startsWith("TZID:") }
    if (vtz) {
        String tzid2 = vtz.substring(5).trim().replace("\"", "")
        TimeZone tz2 = resolveTimeZone(tzid2, null)
        if (tz2) return tz2
    }
    return hubTz()
}

private boolean isEligible(Map ev) {
    if (ev.status?.toUpperCase() == "CANCELLED") return false
    if (ev.allDay && !triggerAllDay) return false
    if (triggerBusyOnly && ev.transp?.toUpperCase() == "TRANSPARENT") return false
    if (excludeTentative && ev.status?.toUpperCase() == "TENTATIVE") return false
    List<String> includes = parseKeywords(includeKeywords)
    List<String> excludes = parseKeywords(excludeKeywords)
    String hay = ((ev.summary ?: "") + " " + (ev.location ?: "")).toLowerCase()
    if (includes && !includes.any { hay.contains(it) }) return false
    if (excludes && excludes.any { hay.contains(it) }) return false
    return true
}

private List<String> parseKeywords(String s) { s ? s.split(",").collect { it.trim().toLowerCase() }.findAll { it } : [] }

private void scheduleNextTransition(List activeNow, Map next, Long nowMs) {
    Long targetMs = activeNow ? activeNow.sort { it.effEndMs }[0].effEndMs : (next ? next.effStartMs : null)
    if (targetMs) {
        int delta = Math.max(2, Math.round((targetMs - nowMs) / 1000.0).toInteger())
        state.nextTransitionAtMs = targetMs
        runIn(delta, "pollTransition")
    }
}

private void scheduleNextPoll() { runIn(Math.max(30, safeInt(pollSeconds, 900)), "pollRegular") }

private List<Map> parseIcs(String icsText, TimeZone calendarTz) {
    List<Map> events = []
    Map cur = null
    unfoldLines(icsText).each { line ->
        if (line == "BEGIN:VEVENT") cur = [props: [:], attendees: []]
        else if (line == "END:VEVENT") { if (cur) { Map ev = buildEvent(cur, calendarTz); if (ev) events << ev }; cur = null }
        else if (cur) {
            int idx = line.indexOf(":")
            if (idx > 0) {
                String left = line.substring(0, idx)
                String name = left.split(";")[0]
                Map params = [:]
                if (left.contains(";")) left.split(";").drop(1).each { 
                    def p = it.split("="); if (p.size()==2) params[p[0]] = p[1] 
                }
                cur.props[name] = [value: line.substring(idx + 1), params: params]
            }
        }
    }
    return events
}

private Map buildEvent(Map raw, TimeZone calendarTz) {
    Map p = raw.props
    if (!p.DTSTART) return null
    TimeZone tzStart = tzFromParams(p.DTSTART.params) ?: calendarTz
    Date start = parseICalDate(p.DTSTART.value, tzStart, calendarTz)
    if (!start) return null
    Date end = p.DTEND ? parseICalDate(p.DTEND.value, tzFromParams(p.DTEND.params) ?: tzStart, calendarTz) : new Date(start.time + 1800000L)
    return [
        uid: p.UID?.value ?: "",
        summary: unescapeIcsText(p.SUMMARY?.value ?: ""),
        location: unescapeIcsText(p.LOCATION?.value ?: ""),
        status: p.STATUS?.value ?: "",
        transp: p.TRANSP?.value ?: "",
        startMs: start.time,
        endMs: end.time,
        durationMs: end.time - start.time,
        allDay: isAllDayValue(p.DTSTART.value),
        rrule: p.RRULE?.value,
        recurrenceIdMs: p["RECURRENCE-ID"] ? parseICalDate(p["RECURRENCE-ID"].value, tzStart, calendarTz)?.time : null,
        recTzId: tzStart.getID()
    ]
}

private TimeZone tzFromParams(Map params) {
    String tzid = params?.TZID
    return resolveTimeZone(tzid, null)
}

private Date parseICalDate(String v, TimeZone tzContext, TimeZone calTz) {
    if (!v) return null
    TimeZone tz = v.endsWith("Z") ? TimeZone.getTimeZone("UTC") : (tzContext ?: calTz ?: hubTz())
    String fmt = v.contains("T") ? (v.endsWith("Z") ? "yyyyMMdd'T'HHmmss'Z'" : "yyyyMMdd'T'HHmmss") : "yyyyMMdd"
    try { SimpleDateFormat sdf = new SimpleDateFormat(fmt); sdf.setTimeZone(tz); return sdf.parse(v) } catch (e) { return null }
}

private boolean isAllDayValue(String v) { v && !v.contains("T") && v.length() == 8 }

private List<String> unfoldLines(String text) {
    List out = []
    String cur = null
    text.replace("\r\n", "\n").split("\n").each { line ->
        if (line.startsWith(" ") || line.startsWith("\t")) { if (cur != null) cur += line.substring(1) }
        else { if (cur != null) out << cur; cur = line.trim() }
    }
    if (cur != null) out << cur
    return out
}

private String unescapeIcsText(String s) { s?.replace("\\n", "\n")?.replace("\\,", ",")?.replace("\\;", ";") ?: "" }

private Map parseRrule(String rrule) {
    Map m = [:]
    rrule?.split(";")?.each { part ->
        def p = part.split("=", 2)
        if (p.size() == 2) {
            String k = (p[0] ?: "").trim().toUpperCase()
            String v = (p[1] ?: "").trim()
            // Outlook sometimes quotes RRULE values (e.g., FREQ="MONTHLY")
            if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
                v = v.substring(1, v.length() - 1)
            }
            m[k] = v
        }
    }
    return m
}

private Long parseUntilMs(String raw, TimeZone tz, TimeZone calTz) { parseICalDate(raw, tz, calTz)?.time }

private String dowToByday(int dow) { ["", "SU", "MO", "TU", "WE", "TH", "FR", "SA"][dow] }

private TimeZone hubTz() { location?.timeZone ?: TimeZone.getDefault() }

private long epochDay(Long ms, TimeZone tz) {
    Calendar c = Calendar.getInstance(tz)
    c.setTimeInMillis(ms)
    return java.time.LocalDate.of(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH)).toEpochDay()
}

private List<Integer> parseIntList(String s) {
    if (!s) return []
    return s.split(",")*.trim().findAll { it }*.toInteger()
}

private int lastDayOfMonth(Calendar c) {
    Calendar t = (Calendar)c.clone()
    t.set(Calendar.DAY_OF_MONTH, 1)
    t.add(Calendar.MONTH, 1)
    t.add(Calendar.DATE, -1)
    return t.get(Calendar.DAY_OF_MONTH)
}

private boolean matchesBydayToken(Calendar curr, String token) {
    if (!token) return false
    String dayPart = token.replaceAll("[^A-Z]", "")
    String numPart = token.replaceAll("[A-Z]", "")
    if (dowToByday(curr.get(Calendar.DAY_OF_WEEK)) != dayPart) return false
    if (!numPart) return true

    int occurrence = numPart.toInteger()
    if (occurrence > 0) {
        return ((curr.get(Calendar.DAY_OF_MONTH) - 1) / 7).toInteger() + 1 == occurrence
    } else {
        Calendar nextWeek = (Calendar)curr.clone()
        nextWeek.add(Calendar.DATE, 7)
        return nextWeek.get(Calendar.MONTH) != curr.get(Calendar.MONTH)
    }
}

private boolean matchesMonthly(Calendar curr, Calendar startCal, Map rr) {
    // BYMONTHDAY (supports negative numbers like -1 for last day of month)
    if (rr.BYMONTHDAY) {
        int dom = curr.get(Calendar.DAY_OF_MONTH)
        int last = lastDayOfMonth(curr)
        for (Integer d : parseIntList(rr.BYMONTHDAY)) {
            if (d > 0 && dom == d) return true
            if (d < 0 && dom == (last + d + 1)) return true
        }
        return false
    }

    // BYDAY + optional BYSETPOS (common Outlook "nth weekday" rules)
    if (rr.BYDAY) {
        List<String> tokens = rr.BYDAY.split(",")*.trim().findAll { it }
        List<Integer> setpos = rr.BYSETPOS ? parseIntList(rr.BYSETPOS) : []

        if (setpos) {
            // Build ordered list of all days in this month that match any weekday in BYDAY (ignoring numeric prefixes).
            Set<String> dows = tokens.collect { it.replaceAll("[^A-Z]", "") }.toSet()
            Calendar t = (Calendar)curr.clone()
            t.set(Calendar.DAY_OF_MONTH, 1)
            t.set(Calendar.HOUR_OF_DAY, 12); t.set(Calendar.MINUTE, 0); t.set(Calendar.SECOND, 0); t.set(Calendar.MILLISECOND, 0)

            List<Integer> matchingDom = []
            int last = lastDayOfMonth(t)
            for (int d = 1; d <= last; d++) {
                t.set(Calendar.DAY_OF_MONTH, d)
                if (dows.contains(dowToByday(t.get(Calendar.DAY_OF_WEEK)))) matchingDom << d
            }

            int dom = curr.get(Calendar.DAY_OF_MONTH)
            for (Integer p : setpos) {
                int idx = (p > 0) ? (p - 1) : (matchingDom.size() + p)
                if (idx >= 0 && idx < matchingDom.size() && dom == matchingDom[idx]) return true
            }
            return false
        }

        // No BYSETPOS – treat each token independently (e.g., 2TU, -1FR, or plain TU).
        for (String tok : tokens) {
            if (matchesBydayToken(curr, tok)) return true
        }
        return false
    }

    // Default: same day-of-month as DTSTART.
    return curr.get(Calendar.DAY_OF_MONTH) == startCal.get(Calendar.DAY_OF_MONTH)
}

private String formatEventLine(Map ev, TimeZone tz) {
    SimpleDateFormat dF = new SimpleDateFormat("EEE MMM d")
    SimpleDateFormat tF = new SimpleDateFormat("h:mm a")
    dF.setTimeZone(tz); tF.setTimeZone(tz)
    return ev.allDay ? "${dF.format(new Date(ev.effStartMs))} (All-day) ${ev.summary}" : "${dF.format(new Date(ev.effStartMs))} ${tF.format(new Date(ev.effStartMs))} – ${tF.format(new Date(ev.effEndMs))} ${ev.summary}"
}

private String formatEventLineForList(Map ev, TimeZone tz, boolean showLoc) {
    String line = formatEventLine(ev, tz)
    if (showLoc && ev.location) line += " @ ${ev.location}"
    return line
}

private String fmtStamp(Date d, TimeZone tz) {
    SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM d, h:mm:ss a z")
    sdf.setTimeZone(tz); return sdf.format(d)
}

private Integer safeInt(v, Integer defVal) {
    try { return v ? v.toInteger() : defVal } catch (e) { return defVal }
}

private void debugAdd(String msg) {
    if (!(debugLogging == true)) return
    String stamp = new SimpleDateFormat("MM-dd HH:mm:ss").format(new Date())
    String line = "${stamp} ${msg}"
    log.debug(line)
    String buf = (state.debugBuf instanceof String) ? (state.debugBuf as String) : ""
    buf = buf ? (buf + "\n" + line) : line
    Integer max = safeInt(debugMaxChars, 6000)
    if (buf.length() > max) buf = buf.substring(buf.length() - max)
    state.debugBuf = buf
    sendEvent(name: "rawDebug", value: buf)
}