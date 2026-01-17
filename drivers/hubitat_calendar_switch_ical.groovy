/**
 *  HE iCal Switch
 *
 *  Author: Chris Truitt
 *  GitHub:  https://github.com/truittchris/he_ical_switch
 *  Contact: hello@christruitt.com
 *  Find this useful?  Tips are gladly accepted!
 *  https://www.christruitt.com/tip-jar
 *
 *  Summary
 *   - Follows an iCal (ICS) URL and drives a Hubitat switch based on eligible events
 *   - Uses the Hubitat hub timezone for display and scheduling (no timezone override required)
 *   - Supports Outlook / Microsoft 365 calendar feeds via calendar-level timezone parsing:
 *       X-WR-TIMEZONE and VTIMEZONE
 *   - Expands common Outlook/M365 weekly RRULEs (BYDAY, INTERVAL, UNTIL, WKST) into concrete instances
 *     within the poll window, and applies RECURRENCE-ID overrides.
 *
 *  Time handling
 *   - DTSTART/DTEND supported forms:
 *       1) UTC timestamps with "Z"
 *       2) TZID=... parameters
 *       3) Floating times (no TZID, no Z) interpreted using calendar timezone if present, else hub timezone
 *   - Internally uses epoch milliseconds (absolute time); formatted output is always in hub timezone
 *
 *  Scheduling model
 *   - Regular poll runs on pollSeconds cadence (fetch + parse)
 *   - Transition poll runs at event boundaries and must not be throttled by pollSeconds
 *   - A small anti-spam throttle (5s) prevents rapid manual or duplicate scheduling calls
 *
 *  License / Attribution
 *   - Free for public use and redistribution, including modified versions
 *   - Attribution must remain intact (author name, website, GitHub link, contact)
 *
 *  Disclaimer
 *   - Provided "as is" without warranty. Use at your own risk.
 */

import java.text.SimpleDateFormat
import java.util.Calendar

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

    input name: "includeKeywords", type: "string", title: "Include keywords (comma-separated) – matches SUMMARY/LOCATION", required: false
    input name: "excludeKeywords", type: "string", title: "Exclude keywords (comma-separated) – matches SUMMARY/LOCATION", required: false

    input name: "startOffsetMin", type: "number", title: "Start offset (minutes). Negative = earlier, positive = later", defaultValue: 0
    input name: "endOffsetMin", type: "number", title: "End offset (minutes). Negative = earlier, positive = later", defaultValue: 0

    input name: "nextListCount", type: "number", title: "Next events list size (for debug)", defaultValue: 10
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

    sendEvent(name: "lastStatus", value: "Initializing")
    debugAdd("Initialize: pollSeconds=${safeInt(pollSeconds, 900)}, includePastHours=${safeInt(includePastHours, 6)}, horizonDays=${safeInt(horizonDays, 3)}, maxEvents=${safeInt(maxEvents, 80)}")
    debugAdd("Filters: triggerBusyOnly=${!!triggerBusyOnly}, excludeTentative=${!!excludeTentative}, excludeDeclinedIfPresent=${!!excludeDeclinedIfPresent}, triggerAllDay=${!!triggerAllDay}")
    debugAdd("Keywords: include='${includeKeywords ?: ""}', exclude='${excludeKeywords ?: ""}', offsets: start=${safeInt(startOffsetMin, 0)}m end=${safeInt(endOffsetMin, 0)}m")
    debugAdd("Next list: count=${safeInt(nextListCount, 10)}, showLocation=${!!nextListShowLocation}")
    debugAdd("Hub TZ: ${hubTz()?.getID()}")

    runIn(2, "pollTransition")
}

def refresh() { pollTransition() }
def showNext() { pollTransition() }

def clearDebug() {
    state.debugBuf = ""
    sendEvent(name: "rawDebug", value: "")
}

def on() {
    sendEvent(name: "switch", value: "on")
    debugAdd("Switch manually turned ON.")
}

def off() {
    sendEvent(name: "switch", value: "off")
    debugAdd("Switch manually turned OFF.")
}

/* ---------------------------
   Poll entrypoints
---------------------------- */

def pollRegular() {
    poll([force: false, reason: "regular"])
}

def pollTransition() {
    poll([force: true, reason: "transition"])
}

/* ---------------------------
   Main poll logic
---------------------------- */

def poll(Map data = null) {
    boolean force = (data?.force == true)
    String reason = (data?.reason ?: "manual") as String

    if (!icsUrl?.trim()) {
        sendEvent(name: "lastStatus", value: "Missing ICS URL")
        debugAdd("Poll aborted (${reason}): ICS URL is empty.")
        scheduleNextPoll()
        return
    }

    Long nowMs = now()

    // Small anti-spam throttle only. Transition polls must not be blocked by pollSeconds.
    Long lastPoll = (state.lastPollMs instanceof Long) ? (state.lastPollMs as Long) : 0L
    Integer minGapMs = 5000
    if (!force && lastPoll && (nowMs - lastPoll) < minGapMs) {
        Long waitMs = minGapMs - (nowMs - lastPoll)
        debugAdd("Poll throttled (${reason}); next attempt in ~${Math.round(waitMs / 1000)}s")
        runIn(Math.max(2, Math.round(waitMs / 1000) as Integer), "pollRegular")
        return
    }
    state.lastPollMs = nowMs

    sendEvent(name: "lastStatus", value: "Fetching")
    debugAdd("Fetching ICS (${reason}): ${icsUrl}")

    String body = null
    Integer statusCode = null

    try {
        httpGet([uri: icsUrl, timeout: 25]) { resp ->
            statusCode = resp?.status
            if (resp?.data != null) {
                try {
                    body = (resp.data instanceof String) ? (resp.data as String) : resp.data.getText("UTF-8")
                } catch (e1) {
                    body = resp.data?.toString()
                }
            }
        }
    } catch (e) {
        sendEvent(name: "lastStatus", value: "Fetch exception")
        debugAdd("Fetch exception (${reason}): ${e}")
        scheduleNextPoll()
        return
    }

    sendEvent(name: "lastFetch", value: fmtStamp(new Date(), hubTz()))
    debugAdd("Fetch status=${statusCode}, chars=${body ? body.length() : 0}")

    if (!body || statusCode != 200) {
        sendEvent(name: "lastStatus", value: "Fetch failed")
        debugAdd("Fetch failed: status=${statusCode}, bodyPresent=${body != null}")
        scheduleNextPoll()
        return
    }

    if (!body.contains("BEGIN:VCALENDAR") || !body.contains("BEGIN:VEVENT")) {
        sendEvent(name: "lastStatus", value: "Invalid ICS")
        debugAdd("Invalid ICS: missing VCALENDAR/VEVENT. Head='${safeHead(body)}'")
        scheduleNextPoll()
        return
    }

    sendEvent(name: "lastStatus", value: "Parsing")

    TimeZone calTz = detectCalendarTimeZone(body)
    state.calendarTzid = calTz?.getID()
    sendEvent(name: "calendarTz", value: state.calendarTzid)
    debugAdd("Calendar TZ detected: ${state.calendarTzid ?: "none"}")

    List<Map> events = parseIcs(body, calTz)
    debugAdd("Parsed VEVENT count=${events.size()}")

    Long windowStart = nowMs - (safeInt(includePastHours, 6) * 3600000L)
    Long windowEnd   = nowMs + (safeInt(horizonDays, 3) * 86400000L)

    events = expandRecurringEvents(events, windowStart, windowEnd, calTz)
    debugAdd("After RRULE expansion: event instances=${events.size()}")

    Long startOffsetMs = safeInt(startOffsetMin, 0) * 60000L
    Long endOffsetMs   = safeInt(endOffsetMin, 0) * 60000L

    List<Map> eligible = events.findAll { ev ->
        Long s = (ev?.startMs as Long)
        Long e = (ev?.endMs as Long)
        if (!s || !e) return false

        Long es = s + startOffsetMs
        Long ee = e + endOffsetMs
        if (ee < es) return false

        (ee >= windowStart) && (es <= windowEnd)
    }.collect { ev ->
        ev.effStartMs = (ev.startMs as Long) + startOffsetMs
        ev.effEndMs   = (ev.endMs as Long) + endOffsetMs
        return ev
    }.findAll { ev ->
        isEligible(ev)
    }.sort { a, b -> (a.effStartMs as Long) <=> (b.effStartMs as Long) }

    Integer cap = safeInt(maxEvents, 80)
    if (eligible.size() > cap) eligible = eligible.take(cap)

    List<Map> activeNow = eligible.findAll { ev ->
        (ev.effStartMs as Long) <= nowMs && nowMs < (ev.effEndMs as Long)
    }

    Map next = eligible.find { ev ->
        (ev.effStartMs as Long) > nowMs
    }

    boolean isActiveNow = (activeNow && activeNow.size() > 0)
    sendEvent(name: "active", value: isActiveNow)

    Map governing = null
    if (isActiveNow) {
        governing = activeNow.sort { a, b -> (a.effEndMs as Long) <=> (b.effEndMs as Long) }[0]
    }

    sendEvent(name: "activeSummary", value: governing ? formatEventLine(governing, hubTz()) : null)
    sendEvent(name: "nextSummary", value: next ? formatEventLine(next, hubTz()) : null)

    Integer n = Math.max(0, safeInt(nextListCount, 10))
    String nextEventsText = ""
    if (n > 0) {
        List<Map> upcoming = eligible.findAll { ev ->
            (ev.effEndMs as Long) >= nowMs
        }.take(n)

        nextEventsText = upcoming.collect { ev ->
            "• " + formatEventLineForList(ev, hubTz(), (nextListShowLocation == true))
        }.join("\n")
    }
    sendEvent(name: "nextEvents", value: nextEventsText)

    String desiredSwitch = isActiveNow ? "on" : "off"
    String currentSwitch = device.currentValue("switch") as String
    if (desiredSwitch != currentSwitch) {
        sendEvent(name: "switch", value: desiredSwitch)
        debugAdd("Switch set to ${desiredSwitch} (eligibleActive=${isActiveNow})")
    }

    scheduleNextTransition(activeNow, next, nowMs)

    sendEvent(name: "lastStatus", value: "OK")
    scheduleNextPoll()
}

/* ---------------------------
   Calendar timezone detection
---------------------------- */

private TimeZone detectCalendarTimeZone(String icsText) {
    TimeZone hub = hubTz()
    if (!icsText) return hub

    List<String> lines = unfoldLines(icsText)

    String xwr = lines.find { it?.startsWith("X-WR-TIMEZONE:") }
    if (xwr) {
        String tzid = xwr.substring("X-WR-TIMEZONE:".length()).trim()
        tzid = tzid.replace("\"", "")
        TimeZone tz = TimeZone.getTimeZone(tzid)
        if (tz && tz.getID() != "GMT") return tz
    }

    Integer vtzIdx = lines.findIndexOf { it == "BEGIN:VTIMEZONE" }
    if (vtzIdx >= 0) {
        for (int i = vtzIdx; i < Math.min(lines.size(), vtzIdx + 80); i++) {
            String line = lines[i]
            if (line?.startsWith("TZID:")) {
                String tzid = line.substring("TZID:".length()).trim().replace("\"", "")
                TimeZone tz = TimeZone.getTimeZone(tzid)
                if (tz && tz.getID() != "GMT") return tz
            }
            if (line == "END:VTIMEZONE") break
        }
    }

    return hub
}

/* ---------------------------
   Eligibility logic
---------------------------- */

private boolean isEligible(Map ev) {
    String status = (ev?.status ?: "") as String
    if (status?.toUpperCase() == "CANCELLED") return false

    if ((ev?.allDay == true) && !(triggerAllDay == true)) return false

    if (triggerBusyOnly == true) {
        String transp = (ev?.transp ?: "") as String
        if (transp?.toUpperCase() == "TRANSPARENT") return false
    }

    if (excludeTentative == true) {
        if (status?.toUpperCase() == "TENTATIVE") return false
    }

    if (excludeDeclinedIfPresent == true) {
        List<String> partstats = (ev?.partstats instanceof List) ? (ev.partstats as List<String>) : []
        boolean hasDeclined = partstats.any { ps -> (ps ?: "").toUpperCase() == "DECLINED" }
        if (hasDeclined) return false
    }

    List<String> includes = parseKeywords(includeKeywords)
    List<String> excludes = parseKeywords(excludeKeywords)

    String hay = ((ev?.summary ?: "") + " " + (ev?.location ?: "")).toString().toLowerCase()

    if (includes && includes.size() > 0) {
        boolean anyMatch = includes.any { kw -> hay.contains(kw) }
        if (!anyMatch) return false
    }

    if (excludes && excludes.size() > 0) {
        boolean anyExclude = excludes.any { kw -> hay.contains(kw) }
        if (anyExclude) return false
    }

    return true
}

private List<String> parseKeywords(String s) {
    if (!s) return []
    return (s.split(",") as List<String>)
        .collect { it?.trim()?.toLowerCase() }
        .findAll { it }
}

/* ---------------------------
   Transition scheduling
---------------------------- */

private void scheduleNextTransition(List<Map> activeNow, Map next, Long nowMs) {
    Long targetMs = null
    String why = null

    if (activeNow && activeNow.size() > 0) {
        Map soonestEnd = activeNow.sort { a, b -> (a.effEndMs as Long) <=> (b.effEndMs as Long) }[0]
        targetMs = soonestEnd.effEndMs as Long
        why = "active-end"
    } else if (next) {
        targetMs = next.effStartMs as Long
        why = "next-start"
    }

    if (!targetMs) {
        state.nextTransitionAtMs = null
        debugAdd("No upcoming transition found.")
        return
    }

    Long deltaMs = targetMs - nowMs
    Integer seconds = (deltaMs <= 0) ? 2 : Math.max(2, Math.round(deltaMs / 1000.0) as Integer)

    Long last = (state.nextTransitionAtMs instanceof Long) ? (state.nextTransitionAtMs as Long) : null
    if (last && Math.abs(last - targetMs) < 1500) {
        debugAdd("Transition unchanged (${why}) at ${fmtStamp(new Date(targetMs), hubTz())}")
        return
    }

    state.nextTransitionAtMs = targetMs
    runIn(seconds, "pollTransition")
    debugAdd("Scheduled transition (${why}) in ${seconds}s at ${fmtStamp(new Date(targetMs), hubTz())}")
}

private void scheduleNextPoll() {
    Integer s = Math.max(30, safeInt(pollSeconds, 900))
    runIn(s, "pollRegular")
    debugAdd("Scheduled regular poll in ${s}s")
}

/* ---------------------------
   ICS parsing
---------------------------- */

private List<Map> parseIcs(String icsText, TimeZone calendarTz) {
    List<String> lines = unfoldLines(icsText)

    List<Map> events = []
    Map cur = null

    lines.each { String line ->
        if (line == "BEGIN:VEVENT") {
            cur = [props: [:], attendees: []]
            return
        }
        if (line == "END:VEVENT") {
            if (cur) {
                Map ev = buildEvent(cur, calendarTz)
                if (ev) events << ev
            }
            cur = null
            return
        }
        if (!cur) return

        Integer idx = line.indexOf(":")
        if (idx <= 0) return

        String left = line.substring(0, idx)
        String value = line.substring(idx + 1)

        String name = left
        Map params = [:]

        if (left.contains(";")) {
            List<String> parts = left.split(";") as List<String>
            name = parts[0]
            parts.drop(1).each { p ->
                Integer eIdx = p.indexOf("=")
                if (eIdx > 0) {
                    String k = p.substring(0, eIdx)
                    String v = p.substring(eIdx + 1)
                    params[k] = v
                } else {
                    params[p] = true
                }
            }
        }

        if (name == "ATTENDEE") {
            cur.attendees << [value: value, params: params]
        } else {
            cur.props[name] = [value: value, params: params]
        }
    }

    return events
}

private Map buildEvent(Map raw, TimeZone calendarTz) {
    Map p = raw?.props ?: [:]

    Map ds = p.DTSTART
    Map de = p.DTEND
    if (!ds?.value) return null

    String status = ((p.STATUS?.value ?: "") as String).trim()
    String transp = ((p.TRANSP?.value ?: "") as String).trim()

    TimeZone tzStart = tzFromParams(ds?.params)
    TimeZone tzEnd   = tzFromParams(de?.params)

    Date start = parseICalDate(ds.value as String, tzStart, calendarTz)
    if (!start) {
        debugAdd("Dropped VEVENT: unparseable DTSTART='${ds?.value}' TZID='${ds?.params?.TZID}' uid='${p.UID?.value ?: ""}' summary='${p.SUMMARY?.value ?: ""}'")
        return null
    }

    Date end
    if (de?.value) {
        end = parseICalDate(de.value as String, tzEnd ?: tzStart, calendarTz)
    } else {
        if (isAllDayValue(ds.value as String)) end = new Date(start.time + 86400000L)
        else end = new Date(start.time + 1800000L)
    }

    if (!start || !end) return null
    if (end.time < start.time) return null

    String summary = unescapeIcsText((p.SUMMARY?.value ?: "") as String)
    String location = unescapeIcsText((p.LOCATION?.value ?: "") as String)
    String uid = (p.UID?.value ?: "") as String

    String rrule = ((p.RRULE?.value ?: "") as String).trim()
    if (!rrule) rrule = null

    Long recurrenceIdMs = null
    if (p["RECURRENCE-ID"]?.value) {
        Map rid = p["RECURRENCE-ID"]
        TimeZone tzRid = tzFromParams(rid?.params) ?: tzStart
        Date ridDate = parseICalDate(rid.value as String, tzRid, calendarTz)
        if (ridDate) recurrenceIdMs = ridDate.time
    }

    List<String> partstats = []
    List<Map> attendees = (raw?.attendees instanceof List) ? (raw.attendees as List<Map>) : []
    attendees.each { at ->
        Map prm = at?.params ?: [:]
        String ps = prm?.PARTSTAT
        if (ps) partstats << ps
    }

    Long durationMs = end.time - start.time
    TimeZone tzRec = tzStart ?: (calendarTz ?: hubTz())

    return [
        uid           : uid,
        summary       : summary,
        location      : location,
        status        : status,
        transp        : transp,
        partstats     : partstats,
        startMs       : start.time,
        endMs         : end.time,
        durationMs    : durationMs,
        allDay        : isAllDayValue(ds.value as String),

        rrule         : rrule,
        recurrenceIdMs: recurrenceIdMs,
        recTzId       : tzRec?.getID()
    ]
}

private TimeZone tzFromParams(Map params) {
    if (!params) return null
    String tzid = params.TZID
    if (!tzid) return null
    tzid = tzid.replace("\"", "")
    TimeZone tz = TimeZone.getTimeZone(tzid)
    if (tz?.getID() == "GMT" && tzid != "GMT") return null
    return tz
}

private Date parseICalDate(String rawVal, TimeZone tzContext, TimeZone calendarTz) {
    if (!rawVal) return null

    String v = rawVal.trim()
    TimeZone hub = hubTz()

    if (v.endsWith("Z") && v.contains("T")) {
        Date d = tryParse(v, "yyyyMMdd'T'HHmmss'Z'", TimeZone.getTimeZone("UTC"))
        if (d) return d
        d = tryParse(v, "yyyyMMdd'T'HHmm'Z'", TimeZone.getTimeZone("UTC"))
        if (d) return d
        return null
    }

    if (!v.contains("T")) {
        TimeZone tz = tzContext ?: (calendarTz ?: hub)
        return tryParse(v, "yyyyMMdd", tz)
    }

    TimeZone tz = tzContext ?: (calendarTz ?: hub)
    Date d = tryParse(v, "yyyyMMdd'T'HHmmss", tz)
    if (d) return d
    d = tryParse(v, "yyyyMMdd'T'HHmm", tz)
    if (d) return d

    return null
}

private Date tryParse(String v, String pattern, TimeZone tz) {
    try {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern)
        sdf.setLenient(false)
        sdf.setTimeZone(tz)
        return sdf.parse(v)
    } catch (e) {
        return null
    }
}

private boolean isAllDayValue(String rawVal) {
    if (!rawVal) return false
    String v = rawVal.trim()
    (!v.contains("T")) && (v.length() == 8)
}

private List<String> unfoldLines(String text) {
    List<String> raw = text.replace("\r\n", "\n").split("\n") as List<String>
    List<String> out = []
    String cur = null

    raw.each { String line ->
        if (line == null) return
        if (line.startsWith(" ") || line.startsWith("\t")) {
            if (cur != null) cur = cur + line.substring(1)
        } else {
            if (cur != null) out << cur
            cur = line.trim()
        }
    }
    if (cur != null) out << cur
    return out
}

private String unescapeIcsText(String s) {
    if (s == null) return ""
    String out = s
    out = out.replace("\\n", "\n")
    out = out.replace("\\N", "\n")
    out = out.replace("\\\\", "\\")
    out = out.replace("\\,", ",")
    out = out.replace("\\;", ";")
    return out
}

/* ---------------------------
   RRULE expansion (WEEKLY)
---------------------------- */

private List<Map> expandRecurringEvents(List<Map> events, Long windowStart, Long windowEnd, TimeZone calendarTz) {
    if (!events) return []

    Map<String, Map> overrides = [:]
    events.findAll { it?.recurrenceIdMs instanceof Long }.each { ev ->
        String k = "${ev.uid}@@${ev.recurrenceIdMs as Long}"
        overrides[k] = ev
    }

    List<Map> out = []

    events.each { ev ->
        if (!ev) return
        boolean isMasterWithRrule = (ev.rrule instanceof String) && !(ev.recurrenceIdMs instanceof Long)
        if (!isMasterWithRrule) out << ev
    }

    events.findAll { it?.rrule && !(it?.recurrenceIdMs instanceof Long) }.each { master ->
        out.addAll(expandWeeklyMaster(master, windowStart, windowEnd, calendarTz, overrides))
    }

    out = out.findAll { it?.startMs && it?.endMs }
             .sort { a, b -> (a.startMs as Long) <=> (b.startMs as Long) }

    return out
}

private List<Map> expandWeeklyMaster(Map master, Long windowStart, Long windowEnd, TimeZone calendarTz, Map<String, Map> overrides) {
    List<Map> out = []
    String rrule = (master.rrule as String)
    Map rr = parseRrule(rrule)
    if (!rr) return out

    if ((rr.FREQ ?: "").toUpperCase() != "WEEKLY") {
        debugAdd("RRULE unsupported FREQ='${rr.FREQ}' uid='${master.uid}' summary='${master.summary}'")
        return out
    }

    Integer interval = safeInt(rr.INTERVAL, 1)
    if (interval < 1) interval = 1

    List<String> byday = []
    if (rr.BYDAY) {
        byday = (rr.BYDAY as String).split(",").collect { it.trim().toUpperCase() }.findAll { it }
    }

    TimeZone tzRec = tzFromId(master.recTzId as String) ?: (calendarTz ?: hubTz())

    Calendar cStart = Calendar.getInstance(tzRec)
    cStart.setTimeInMillis(master.startMs as Long)

    if (!byday || byday.size() == 0) {
        byday = [dowToByday(cStart.get(Calendar.DAY_OF_WEEK))]
    }

    Integer hh = cStart.get(Calendar.HOUR_OF_DAY)
    Integer mm = cStart.get(Calendar.MINUTE)
    Integer ss = cStart.get(Calendar.SECOND)

    Long untilMs = null
    if (rr.UNTIL) {
        untilMs = parseUntilMs(rr.UNTIL as String, tzRec, calendarTz)
    }

    Integer wkst = bydayToCalDow((rr.WKST ?: "SU").toString().trim().toUpperCase())
    if (!wkst) wkst = Calendar.SUNDAY

    Long masterStartMs = master.startMs as Long
    Long durationMs = (master.durationMs instanceof Long) ? (master.durationMs as Long) : ((master.endMs as Long) - (master.startMs as Long))

    Calendar day = Calendar.getInstance(tzRec)
    day.setTimeInMillis(windowStart)
    day.set(Calendar.HOUR_OF_DAY, 0)
    day.set(Calendar.MINUTE, 0)
    day.set(Calendar.SECOND, 0)
    day.set(Calendar.MILLISECOND, 0)

    Calendar endDay = Calendar.getInstance(tzRec)
    endDay.setTimeInMillis(windowEnd)
    endDay.set(Calendar.HOUR_OF_DAY, 23)
    endDay.set(Calendar.MINUTE, 59)
    endDay.set(Calendar.SECOND, 59)
    endDay.set(Calendar.MILLISECOND, 999)

    Long baseWeekStart = weekStartMs(cStart, wkst)

    while (day.getTimeInMillis() <= endDay.getTimeInMillis()) {
        String bd = dowToByday(day.get(Calendar.DAY_OF_WEEK))
        if (byday.contains(bd)) {
            Calendar occ = Calendar.getInstance(tzRec)
            occ.setTimeInMillis(day.getTimeInMillis())
            occ.set(Calendar.HOUR_OF_DAY, hh)
            occ.set(Calendar.MINUTE, mm)
            occ.set(Calendar.SECOND, ss)
            occ.set(Calendar.MILLISECOND, 0)

            Long occStart = occ.getTimeInMillis()
            if (occStart >= masterStartMs) {
                Long occWeekStart = weekStartMs(occ, wkst)
                Long weeks = Math.round((occWeekStart - baseWeekStart) / 604800000.0d)
                if (weeks >= 0 && (weeks % interval) == 0) {
                    if (!untilMs || occStart <= untilMs) {
                        Long occEnd = occStart + durationMs

                        String key = "${master.uid}@@${occStart}"
                        Map ov = overrides[key]

                        if (ov) {
                            if (((ov.status ?: "") as String).toUpperCase() != "CANCELLED") {
                                out << ov
                            }
                        } else {
                            Map gen = [:] + master
                            gen.startMs = occStart
                            gen.endMs = occEnd
                            gen.durationMs = durationMs
                            gen.rrule = null
                            gen.recurrenceIdMs = occStart
                            gen.generated = true
                            out << gen
                        }
                    }
                }
            }
        }
        day.add(Calendar.DATE, 1)
    }

    if (out && out.size() > 0) {
        debugAdd("RRULE expanded '${master.summary}' instances=${out.size()} uid='${master.uid}'")
    }
    return out
}

private Map parseRrule(String rrule) {
    if (!rrule) return null
    Map m = [:]
    rrule.split(";").each { seg ->
        if (!seg) return
        Integer i = seg.indexOf("=")
        if (i > 0) {
            String k = seg.substring(0, i).trim().toUpperCase()
            String v = seg.substring(i + 1).trim()
            if (k) m[k] = v
        }
    }
    return m
}

private Long parseUntilMs(String raw, TimeZone tzRec, TimeZone calendarTz) {
    if (!raw) return null
    String v = raw.trim()
    if (v.endsWith("Z")) {
        Date d = tryParse(v, "yyyyMMdd'T'HHmmss'Z'", TimeZone.getTimeZone("UTC"))
        if (!d) d = tryParse(v, "yyyyMMdd'T'HHmm'Z'", TimeZone.getTimeZone("UTC"))
        return d ? d.time : null
    }
    Date d2 = parseICalDate(v, tzRec, calendarTz)
    return d2 ? d2.time : null
}

private Integer bydayToCalDow(String byday) {
    switch (byday) {
        case "SU": return Calendar.SUNDAY
        case "MO": return Calendar.MONDAY
        case "TU": return Calendar.TUESDAY
        case "WE": return Calendar.WEDNESDAY
        case "TH": return Calendar.THURSDAY
        case "FR": return Calendar.FRIDAY
        case "SA": return Calendar.SATURDAY
        default: return null
    }
}

private String dowToByday(Integer calDow) {
    switch (calDow) {
        case Calendar.SUNDAY: return "SU"
        case Calendar.MONDAY: return "MO"
        case Calendar.TUESDAY: return "TU"
        case Calendar.WEDNESDAY: return "WE"
        case Calendar.THURSDAY: return "TH"
        case Calendar.FRIDAY: return "FR"
        case Calendar.SATURDAY: return "SA"
        default: return "SU"
    }
}

private Long weekStartMs(Calendar any, Integer wkstDow) {
    Calendar c = (Calendar) any.clone()
    while (c.get(Calendar.DAY_OF_WEEK) != wkstDow) {
        c.add(Calendar.DATE, -1)
    }
    c.set(Calendar.HOUR_OF_DAY, 12)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.SECOND, 0)
    c.set(Calendar.MILLISECOND, 0)
    return c.getTimeInMillis()
}

private TimeZone tzFromId(String id) {
    if (!id) return null
    try {
        TimeZone tz = TimeZone.getTimeZone(id)
        if (tz?.getID() == "GMT" && id != "GMT") return null
        return tz
    } catch (e) {
        return null
    }
}

/* ---------------------------
   Formatting + utils
---------------------------- */

private TimeZone hubTz() {
    return location?.timeZone ?: TimeZone.getDefault()
}

private String formatEventLine(Map ev, TimeZone tz) {
    Date s = new Date((ev.effStartMs ? (ev.effStartMs as Long) : (ev.startMs as Long)))
    Date e = new Date((ev.effEndMs ? (ev.effEndMs as Long) : (ev.endMs as Long)))

    if (ev.allDay) {
        String day = fmtDate(s, tz, "EEE MMM d, yyyy")
        return "${day} (All-day) ${ev.summary}${ev.location ? " @ ${ev.location}" : ""}"
    }

    String startStr = fmtDate(s, tz, "EEE MMM d h:mm a")
    String sd = fmtDate(s, tz, "yyyyMMdd")
    String ed = fmtDate(e, tz, "yyyyMMdd")
    String endStr = (sd == ed) ? fmtDate(e, tz, "h:mm a") : fmtDate(e, tz, "EEE MMM d h:mm a")

    String base = "${startStr} – ${endStr} ${ev.summary}"
    if (ev.location) base = base + " @ ${ev.location}"
    return base
}

private String formatEventLineForList(Map ev, TimeZone tz, boolean includeLoc) {
    Date s = new Date((ev.effStartMs ? (ev.effStartMs as Long) : (ev.startMs as Long)))
    Date e = new Date((ev.effEndMs ? (ev.effEndMs as Long) : (ev.endMs as Long)))

    String summary = (ev.summary ?: "").toString()
    String loc = (ev.location ?: "").toString()

    if (ev.allDay) {
        String day = fmtDate(s, tz, "EEE MMM d, yyyy")
        String line = "${day} (All-day) ${summary}"
        if (includeLoc && loc) line = line + " @ ${loc}"
        return line
    }

    String startStr = fmtDate(s, tz, "EEE MMM d h:mm a")
    String sd = fmtDate(s, tz, "yyyyMMdd")
    String ed = fmtDate(e, tz, "yyyyMMdd")
    String endStr = (sd == ed) ? fmtDate(e, tz, "h:mm a") : fmtDate(e, tz, "EEE MMM d h:mm a")

    String line = "${startStr} – ${endStr} ${summary}"
    if (includeLoc && loc) line = line + " @ ${loc}"
    return line
}

private String fmtDate(Date d, TimeZone tz, String pattern) {
    SimpleDateFormat sdf = new SimpleDateFormat(pattern)
    sdf.setTimeZone(tz)
    return sdf.format(d)
}

private String fmtStamp(Date d, TimeZone tz) {
    return fmtDate(d, tz, "EEE MMM d, yyyy h:mm:ss a z")
}

private Integer safeInt(def v, Integer defVal) {
    try {
        if (v == null) return defVal
        if (v instanceof Number) return (v as Number).intValue()
        String s = v.toString().trim()
        if (!s) return defVal
        return Integer.parseInt(s)
    } catch (e) {
        return defVal
    }
}

private String safeHead(String s) {
    if (!s) return ""
    Integer n = Math.min(80, s.length())
    return s.substring(0, n).replace("\n", "\\n").replace("\r", "\\r")
}

private void debugAdd(String msg) {
    if (!(debugLogging == true)) return

    String stamp = fmtDate(new Date(), hubTz(), "MM-dd HH:mm:ss")
    String line = "${stamp} ${msg}"
    log.debug(line)

    String buf = (state.debugBuf instanceof String) ? (state.debugBuf as String) : ""
    buf = buf ? (buf + "\n" + line) : line

    Integer cap = safeInt(debugMaxChars, 6000)
    if (buf.length() > cap) {
        buf = buf.substring(buf.length() - cap)
        Integer nl = buf.indexOf("\n")
        if (nl > 0 && nl < 200) buf = buf.substring(nl + 1)
    }

    state.debugBuf = buf
    sendEvent(name: "rawDebug", value: buf)
}
