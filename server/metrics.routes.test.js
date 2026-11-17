const fs=require('fs'),vm=require('vm'),assert=require('assert');
const source=fs.readFileSync(require('path').join(__dirname,'index.js'),'utf8');
const routes={};const events=[{eventId:'1',eventType:'visit',eventFamily:'public',timestamp:new Date('2026-11-17T04:00:00Z'),timestampUtc:'2026-11-17T04:00:00Z',serverSessionId:'a',sessionId:'x'}, {eventId:'2',eventType:'visit',eventFamily:'public',timestamp:new Date('2026-11-17T03:59:59Z'),timestampUtc:'2026-11-17T03:59:59Z',serverSessionId:'a',sessionId:'y'}];
const auth=()=>{};
const ctx={Date,Map,Set,console:{error(){}},metricsReport:require('./metrics'),readMetricsEvents:async()=>events,ensureAdminAuth:auth,app:{get(path,mw,fn){assert.equal(mw,auth);routes[path]=fn}},prDateToUtc:(s,h=0,m=0,sec=0,ms=0)=>{if(!/^\d{4}-\d{2}-\d{2}$/.test(s))return null;const [y,mo,d]=s.split('-').map(Number);return new Date(Date.UTC(y,mo-1,d,h+4,m,sec,ms))},buildMetricsTimeseries:()=>[]};
vm.createContext(ctx);
vm.runInContext(source.slice(source.indexOf("function prDateToUtc"),source.indexOf("function formatChartHourLabel")),ctx);
vm.runInContext(source.slice(source.indexOf('function getMetricsRangeDefinition'),source.indexOf('function buildMetricsTimeseries')),ctx);
vm.runInContext(source.slice(source.indexOf('async function getMetricsReportInput'),source.indexOf('// --------- PUBLIC GALLERY API')),ctx);
async function call(path,query){const res={headers:{},code:200,setHeader(k,v){this.headers[k]=v},status(code){this.code=code;return this},json(v){this.body=v;return this},send(v){this.body=v;return this}};await routes[path]({query},res);return res;}
(async()=>{
let r=await call('/admin/metrics/overview',{preset:'custom',startDate:'2026-11-17',endDate:'2026-11-17',eventSession:'a'});assert.equal(r.body.summary.visits,1);assert.equal(r.body.range.startUtc,'2026-11-17T04:00:00.000Z');assert.equal(r.body.range.endUtc,'2026-11-18T03:59:59.999Z');
r=await call('/admin/metrics/export',{kind:'events',preset:'custom',startDate:'2026-11-17',endDate:'2026-11-17',eventSession:'a'});assert.equal(r.body.split('\r\n').length,2);
r=await call('/admin/metrics/overview',{preset:'custom',startDate:'2026-02-30',endDate:'2026-03-02'});assert.equal(r.code,400);
ctx.readMetricsEvents=async()=>{throw Error('unavailable')};r=await call('/admin/metrics/overview',{preset:'24h'});assert.equal(r.code,503);assert(!r.body.summary);console.log('Route tests passed: PR midnight, session/export parity, invalid dates, unavailable data, auth middleware');
})().catch(e=>{console.error(e);process.exit(1)});
