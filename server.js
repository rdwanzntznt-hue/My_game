const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000,rooms=new Map();let nid=0;
const srv=http.createServer((q,r)=>{
if(q.url=='/health'){r.end('ok');return}
try{const b=fs.readFileSync(path.join(__dirname,'index.html'));r.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});r.end(b)}
catch(e){r.writeHead(500);r.end('index.html missing')}});
const wss=new WebSocketServer({server:srv,path:'/ws',maxPayload:65536});
const send=(w,o)=>{if(w.readyState==1)w.send(JSON.stringify(o))};
const clean=o=>{o=o&&typeof o=='object'?o:{};return{name:String(o.name||'').slice(0,14),face:Math.abs(+o.face|0)%20,gl:Math.abs(+o.gl|0)%4}};
const members=r=>[r.host,...r.guests];
const list=r=>members(r).map(w=>({peer:w.id,presence:{...w.pres,host:w===r.host?1:0}}));
const bcast=r=>{const l=list(r);members(r).forEach(w=>send(w,{k:'peers',l}))};
wss.on('connection',ws=>{
ws.id='p'+(++nid).toString(36)+Math.random().toString(36).slice(2,6);ws.pres={};ws.room=null;
ws.on('message',buf=>{let d;try{d=JSON.parse(buf)}catch(e){return}if(!d||typeof d!='object')return;
if(d.k=='join'&&!ws.room){const n=String(d.room||'');if(!/^[\w-]{1,40}$/.test(n))return send(ws,{k:'err',e:'net'});
let r=rooms.get(n);
if(d.host){if(r)return send(ws,{k:'err',e:'taken'});r={n,max:d.max==2?2:4,host:ws,guests:[]};rooms.set(n,r)}
else{if(!r)return send(ws,{k:'err',e:'nohost'});if(r.guests.length+1>=r.max)return send(ws,{k:'err',e:'full'});r.guests.push(ws)}
ws.room=r;send(ws,{k:'hi',id:ws.id});bcast(r)}
else if(d.k=='pres'&&ws.room){ws.pres=clean(d.d);bcast(ws.room)}
else if(d.k=='m'&&ws.room&&typeof d.e=='string'){const r=ws.room,m={k:'m',e:d.e,d:d.d,from:ws.id};
if(ws===r.host)r.guests.forEach(g=>send(g,m));else send(r.host,m)}});
ws.on('close',()=>{const r=ws.room;if(!r)return;ws.room=null;
if(ws===r.host){rooms.delete(r.n);r.guests.forEach(g=>{g.room=null;send(g,{k:'peers',l:[{peer:g.id,presence:{...g.pres,host:0}}]});g.close()})}
else{r.guests=r.guests.filter(g=>g!==ws);bcast(r)}});
ws.on('error',()=>{})});
srv.listen(PORT,()=>console.log('Tarneeb server on port '+PORT));
