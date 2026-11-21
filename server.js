// Servidor multiplayer do THE ISLE MOBILE (Node.js + ws). Serve o jogo e as salas online.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const HTML = path.join(__dirname, 'index.html');
const RX = /^[a-z0-9][a-z0-9_.-]{0,39}$/, MAXP = 20;
const TOP = new Set(['chat', 'hit', 'pos']);
const KS = new Set(['k','col','x','z','h','sc','sw','lie','nm','msg','mt','i','n','t','d']);

const srv = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.end('ok'); return; }
  fs.readFile(HTML, (e, b) => {
    if (e) { res.writeHead(500); return res.end('index.html nao encontrado'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(b);
  });
});
const wss = new WebSocketServer({ server: srv, path: '/ws', maxPayload: 4096 });
const rooms = new Map(); let nid = 1;

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const roomList = () => [...rooms].map(([name, m]) => ({ name, n: m.size }));
const bcastRooms = () => { const l = roomList(); for (const ws of wss.clients) send(ws, { t: 'rooms', list: l }); };
function clean(o) {
  const r = {}; if (typeof o !== 'object' || !o) return r;
  for (const k in o) {
    if (!KS.has(k)) continue; const v = o[k];
    if (v === null) r[k] = null;
    else if (typeof v === 'number' && isFinite(v)) r[k] = v;
    else if (typeof v === 'string') r[k] = v.slice(0, 80);
  }
  return r;
}
function bc(ws, o) { const m = rooms.get(ws.room); if (!m) return; for (const c of m.values()) if (c.ws !== ws) send(c.ws, o); }
function leave(ws) {
  const name = ws.room; if (!name) return; const m = rooms.get(name); ws.room = null; if (!m) return;
  m.delete(ws.id); for (const c of m.values()) send(c.ws, { t: 'pl', id: ws.id });
  if (!m.size) rooms.delete(name); bcastRooms();
}
wss.on('connection', ws => {
  ws.id = String(nid++); ws.pres = {}; ws.room = null; ws.alive = true; ws.tok = 40;
  ws.on('pong', () => ws.alive = true);
  send(ws, { t: 'hello', id: ws.id }); send(ws, { t: 'rooms', list: roomList() });
  ws.on('message', raw => {
    if (--ws.tok < 0) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'join') {
      if (typeof m.room !== 'string' || !RX.test(m.room)) return send(ws, { t: 'err', c: 'nome' });
      leave(ws); let r = rooms.get(m.room); if (!r) { r = new Map(); rooms.set(m.room, r); }
      if (r.size >= MAXP) return send(ws, { t: 'err', c: 'cheia' });
      ws.room = m.room; ws.pres = clean(m.p || {});
      const others = [...r.values()].map(c => ({ id: c.ws.id, p: c.ws.pres }));
      r.set(ws.id, { ws });
      send(ws, { t: 'joined', room: m.room, peers: others });
      for (const c of r.values()) if (c.ws !== ws) send(c.ws, { t: 'pj', id: ws.id, p: ws.pres });
      bcastRooms();
    } else if (m.t === 'leave') leave(ws);
    else if (m.t === 'pres' && ws.room) {
      const p = clean(m.d || {}); Object.assign(ws.pres, p);
      for (const k in ws.pres) if (ws.pres[k] === null) delete ws.pres[k];
      bc(ws, { t: 'pu', id: ws.id, d: p });
    } else if (m.t === 'emit' && ws.room && TOP.has(m.topic)) {
      const d = clean(m.d || {}); if (typeof d.d === 'number') d.d = Math.min(80, Math.max(0, d.d));
      bc(ws, { t: 'ev', id: ws.id, topic: m.topic, d });
    }
  });
  ws.on('close', () => leave(ws));
});
setInterval(() => { for (const ws of wss.clients) ws.tok = 40; }, 1000);
setInterval(() => { for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); } }, 30000);
srv.listen(PORT, () => console.log('The Isle Mobile rodando na porta ' + PORT));
