const {test}=require('node:test');
const assert=require('node:assert/strict');
const m=require('./metrics');
function e(id,type,session='s1',extra={}) {return {eventId:id,eventType:type,eventFamily:type.includes('photo')?'admin':'public',sessionId:session,serverSessionId:'event1',timestamp:new Date('2026-11-17T16:00:00Z'),timestampUtc:'2026-11-17T16:00:00Z',emailNormalized:'one@example.com',email:'one@example.com',newsletter:'Y',locationLabel:'Puerto Rico / Mayagüez',...extra};}
test('duplicates do not inflate sessions, registrations, uploads or approvals',()=>{
 const data=[e('v1','visit'),e('v2','visit'),e('f1','form'),e('f2','form'),e('u1','upload','s1',{metadata:{fileId:'photo1'}}),e('u2','upload','s1',{metadata:{fileId:'photo1'}}),e('a1','approve_photo','',{metadata:{fileId:'photo1'}})];
 data.push(data[0]);const rows=m.reportingEvents(data),result=m.summarize(rows);
 assert.equal(result.summary.visits,1);assert.equal(result.summary.forms,1);assert.equal(result.summary.uploads,1);assert.equal(result.summary.approvals,1);assert.equal(result.topLocations[0].count,1);assert.equal(result.topLocations[0].percent,100);
});
test('event filter separates reused browser sessions and includes unassigned history',()=>{
 const rows=[e('1','visit'),e('2','visit','s1',{serverSessionId:'event2'}),e('3','visit','s2',{serverSessionId:''})];
 assert.equal(m.reportingEvents(rows).length,3);assert.equal(m.filterSession(rows,'event2').length,1);assert.equal(m.filterSession(rows,'__unassigned__').length,1);assert.equal(m.sessions(rows).length,3);
});
test('newsletter export uses latest recorded consent and neutralizes CSV formulas',()=>{
 const rows=[e('1','form'),e('2','form','s2',{newsletter:'N'}),e('3','form','s3',{email:'=HYPERLINK("bad")',emailNormalized:'other',lastName:'=cmd',newsletter:'Y'})];
 const out=m.exportRows(rows,'newsletter');assert(!out.includes('one@example.com'));assert(out.includes("'=HYPERLINK"));assert(out.includes("'=cmd"));assert.equal(m.newsletterContacts(rows).length,1);
});
test('general export excludes contact information and empty data has only headers',()=>{
 assert(!m.exportRows([e('1','form')],'events').includes('one@example.com'));
 assert.equal(m.exportRows([],'events').split('\r\n').length,1);
 assert.equal(m.summarize([]).summary.forms,0);
});
