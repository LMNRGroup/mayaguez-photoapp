'use strict';
// Pure reporting helpers: never infer people or screen impressions from browser events.
function unique(rows, key) {
  const map = new Map();
  rows.forEach((row, index) => map.set(key(row) || `unidentified:${index}`, row));
  return [...map.values()];
}
function normalizeEvents(rows) {
  return unique(rows.slice().sort((a,b) => a.timestamp-b.timestamp), e => e.eventId);
}
function sessionKey(e) { return e.sessionId ? `${e.serverSessionId || ''}:${e.sessionId}` : ''; }
function reportingEvents(rows) {
  const events = normalizeEvents(rows);
  return [
    ...unique(events.filter(e=>e.eventType==='visit'), sessionKey),
    ...unique(events.filter(e=>e.eventType==='form'), sessionKey),
    ...unique(events.filter(e=>e.eventType==='upload'), e=>e.metadata?.fileId || e.eventId),
    ...unique(events.filter(e=>e.eventType==='approve_photo'), e=>e.metadata?.fileId || e.eventId),
    ...unique(events.filter(e=>e.eventType==='reject_photo'), e=>e.metadata?.fileId || e.eventId),
  ].sort((a,b)=>a.timestamp-b.timestamp);
}
function filterSession(events, id) {
  if (!id) return events;
  return events.filter(e => id === '__unassigned__' ? !e.serverSessionId : e.serverSessionId===id);
}
function sessions(events) {
  const map = new Map();
  for (const e of normalizeEvents(events)) {
    const id = e.serverSessionId || '__unassigned__';
    if (!map.has(id)) map.set(id,{id,firstSeen:e.timestampUtc,lastSeen:e.timestampUtc});
    map.get(id).lastSeen=e.timestampUtc;
  }
  return [...map.values()].sort((a,b)=>b.lastSeen.localeCompare(a.lastSeen));
}
function newsletterContacts(events) {
  // Latest recorded form per email determines consent within the filtered records.
  return unique(events.filter(e=>e.eventType==='form' && e.emailNormalized), e=>e.emailNormalized)
    .filter(e=>e.newsletter==='Y');
}
function summarize(events) {
  const visits=events.filter(e=>e.eventType==='visit');
  const forms=events.filter(e=>e.eventType==='form');
  const uploads=events.filter(e=>e.eventType==='upload');
  const contacts=newsletterContacts(events);
  const locations=new Map();
  for(const e of forms) { const label=e.locationLabel || [e.country,e.region].filter(Boolean).join(' / ') || 'Not provided'; locations.set(label,(locations.get(label)||0)+1); }
  return {
    summary:{visits:visits.length,forms:forms.length,uploads:uploads.length,newsletterOptIns:contacts.length,
      uniqueSessions:new Set(events.filter(e=>e.eventFamily==='public').map(sessionKey).filter(Boolean)).size,
      uniqueEmails:new Set(forms.map(e=>e.emailNormalized).filter(Boolean)).size,
      approvals:events.filter(e=>e.eventType==='approve_photo').length,rejections:events.filter(e=>e.eventType==='reject_photo').length},
    topLocations:[...locations].sort((a,b)=>b[1]-a[1]).map(([label,count])=>({label,count,percent:forms.length ? +(100*count/forms.length).toFixed(1):0})),
    newsletterRows:contacts.slice().reverse().slice(0,12)
  };
}
function csvCell(value) {
  let s=String(value ?? '');
  if (/^[\s]*[=+@\-]/.test(s) || /^[\t\r]/.test(s)) s="'"+s;
  return '"'+s.replace(/"/g,'""')+'"';
}
function csv(rows) { return '\uFEFF'+rows.map(row=>row.map(csvCell).join(',')).join('\r\n'); }
function exportRows(events,kind) {
  if(kind==='newsletter') return csv([
    ['timestamp_utc','timestamp_pr','email','last_name','country','region','consent'],
    ...newsletterContacts(events).map(e=>[e.timestampUtc,e.timestampPr,e.email,e.lastName,e.country,e.region,'Y'])
  ]);
  return csv([
    ['timestamp_utc','timestamp_pr','event_type','event_session','browser_session','country','region','location','ticket'],
    ...events.map(e=>[e.timestampUtc,e.timestampPr,e.eventType,e.serverSessionId,e.sessionId,e.country,e.region,e.locationLabel,e.ticket])
  ]);
}
module.exports={normalizeEvents,reportingEvents,filterSession,sessions,summarize,exportRows,newsletterContacts};
