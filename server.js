// Save the Bob — çok oyunculu sunucu (Node 18+, bağımlılık: ws)
//  - Hesap: sunucu rastgele bir ID (kelime-kelime-sayı) ve rastgele bir isim üretir. Kullanıcı isim seçemez.
//  - Arkadaş: ID ile istek gönderilir, kabul edilince karşılıklı arkadaş olunur.
//  - Oda: hızlı maç (rastgele açık odaya düşer), özel oda (5 haneli kod), arkadaş daveti.
//  - Maç: herkes aynı tavada. Engeller istemcide, sunucunun gönderdiği ortak tohumla (seed) üretilir.
//    Sunucu konumları yayınlar, ölüm sırasını tutar. Son ayakta kalan kazanır.
//  - Sohbet yok: kullanıcı tarafından yazılan metin hiçbir yere iletilmez (moderasyon riski yok).
'use strict';
const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = +process.env.PORT || 8080;
const MAX = 4, MIN = 2, LOBBY_WAIT = 6000, TICK = 100, ROUND_MAX = 240000;
const DATA = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const ORIGINS = (process.env.ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const SKINS = new Set(['broc', 'cuc', 'tom', 'mush', 'egg', 'carrot', 'pep', 'cheese', 'sausage', 'avo', 'eggp']);
const ADJ = ['mutlu', 'hizli', 'tatli', 'cesur', 'sakin', 'neseli', 'sicak', 'gizli', 'parlak', 'kivircik', 'kaygan', 'minik', 'dev', 'ucan', 'sessiz', 'tombul'];
const NOUN = ['brokoli', 'mantar', 'peynir', 'sosis', 'avokado', 'patlican', 'domates', 'havuc', 'biber', 'yumurta', 'kasar', 'tava', 'zeytin', 'tursu', 'lahana', 'bal'];
const CODE_CH = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const accs = new Map();  // id -> hesap
const byTok = new Map(); // gizli token -> hesap
const rooms = new Map(); // kod -> oda

const cap = s => s[0].toUpperCase() + s.slice(1);
const pick = a => a[crypto.randomInt(a.length)];
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const send = (w, o) => { if (w && w.readyState === 1) w.send(JSON.stringify(o)); };
const sendAcc = (a, o) => { if (a && a.ws) send(a.ws, o); };
const msg = (a, key) => sendAcc(a, { t: 'msg', m: key });
const normId = s => String(s || '').trim().toLowerCase().slice(0, 40);

// ---------- kalıcılık (basit JSON dosyası) ----------
function load() {
  try {
    const j = JSON.parse(fs.readFileSync(DATA, 'utf8'));
    for (const a of j.accs || []) {
      if (!a.id || !a.tok) continue;
      const acc = { id: a.id, name: a.name, tok: a.tok, friends: a.friends || [], reqs: a.reqs || [], ws: null, room: null };
      accs.set(acc.id, acc); byTok.set(acc.tok, acc);
    }
  } catch (e) { /* ilk çalıştırma */ }
}
let saveT = null;
function save() {
  clearTimeout(saveT);
  saveT = setTimeout(() => {
    const out = { accs: [...accs.values()].map(a => ({ id: a.id, name: a.name, tok: a.tok, friends: a.friends, reqs: a.reqs })) };
    fs.writeFile(DATA, JSON.stringify(out), () => {});
  }, 800);
}
load();

// ---------- hesap ----------
function newAcc() {
  let id;
  do { id = `${pick(ADJ)}-${pick(NOUN)}-${crypto.randomInt(1000, 10000)}`; } while (accs.has(id));
  const [a, b] = id.split('-');
  const acc = { id, name: `${cap(a)} ${cap(b)}`, tok: crypto.randomBytes(16).toString('hex'), friends: [], reqs: [], ws: null, room: null };
  accs.set(id, acc); byTok.set(acc.tok, acc); save();
  return acc;
}
function profOf(a) {
  return {
    t: 'prof', id: a.id, n: a.name, tok: a.tok,
    fr: a.friends.map(id => {
      const b = accs.get(id), r = b && b.room;
      return { id, n: b ? b.name : '?', on: !!(b && b.ws), code: r && r.st === 'lobby' && r.mem.size < MAX ? r.code : null };
    }),
    rq: a.reqs.map(id => { const b = accs.get(id); return { id, n: b ? b.name : '?' }; })
  };
}
function pushFriends(a) { for (const id of a.friends) { const b = accs.get(id); if (b) sendAcc(b, profOf(b)); } }

// ---------- bağlantı ----------
const wss = new WebSocketServer({
  port: PORT, maxPayload: 1024,
  verifyClient: i => {
    if (!ORIGINS.length) return true;
    const o = i.origin || '';
    // "https://*.poki-gdn.com" gibi joker (*) destekler
    const ok = ORIGINS.some(p => p === o || (p.includes('*') && new RegExp('^' + p.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '[a-z0-9.-]+') + '$', 'i').test(o)));
    if (!ok) console.log('Reddedilen origin (ORIGINS listesine ekle):', o);
    return ok;
  }
});
wss.on('connection', w => {
  w.isAlive = true; w.hits = 0; w.acc = null;
  w.on('pong', () => { w.isAlive = true; });
  const rl = setInterval(() => { w.hits = 0; }, 1000);
  w.on('message', d => {
    if (++w.hits > 30) return w.close(1008, 'rate');
    let m; try { m = JSON.parse(d); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (!w.acc) { if (m.t === 'hello') onHello(w, m); return; }
    onMsg(w.acc, m);
  });
  w.on('close', () => {
    clearInterval(rl);
    const a = w.acc;
    if (a && a.ws === w) { a.ws = null; leaveRoom(a); pushFriends(a); }
  });
  w.on('error', () => {});
});

function onHello(w, m) {
  let a = typeof m.tok === 'string' ? byTok.get(m.tok) : null;
  if (!a) a = newAcc();
  if (a.ws && a.ws !== w) { const o = a.ws; a.ws = null; try { o.close(4000, 'replaced'); } catch (e) {} }
  a.ws = w; w.acc = a;
  send(w, profOf(a));
  // Maç sırasında bağlantısı koptuysa o maçtan düşer (yeniden katılım yok); lobideyse oda bilgisi gönderilir
  if (a.room && a.room.st === 'play') leaveRoom(a);
  else if (a.room) send(w, roomView(a.room));
  pushFriends(a);
}

function onMsg(a, m) {
  switch (m.t) {
    case 'quick': return quick(a);
    case 'create': return create(a);
    case 'join': return joinCode(a, String(m.code || '').toUpperCase().slice(0, 5));
    case 'leave': return leaveRoom(a);
    case 'start': return startByHost(a);
    case 'u': return pos(a, m);
    case 'die': return dieMsg(a);
    case 'fadd': return fadd(a, normId(m.id));
    case 'facc': return facc(a, normId(m.id));
    case 'fno': return fno(a, normId(m.id));
    case 'fdel': return fdel(a, normId(m.id));
    case 'invite': return invite(a, normId(m.id));
  }
}

// ---------- arkadaşlık ----------
function fadd(a, id) {
  const b = accs.get(id);
  if (!b) return msg(a, 'mpNoFriend');
  if (b === a) return msg(a, 'mpSelf');
  if (a.friends.includes(id)) return msg(a, 'mpAlready');
  if (a.reqs.includes(id)) return facc(a, id);
  if (!b.reqs.includes(a.id)) { b.reqs.push(a.id); save(); sendAcc(b, profOf(b)); }
  msg(a, 'mpReqSent');
}
function facc(a, id) {
  if (!a.reqs.includes(id)) return;
  const b = accs.get(id);
  a.reqs = a.reqs.filter(x => x !== id);
  if (b) {
    if (!a.friends.includes(id)) a.friends.push(id);
    if (!b.friends.includes(a.id)) b.friends.push(a.id);
    b.reqs = b.reqs.filter(x => x !== a.id);
    sendAcc(b, profOf(b));
  }
  save(); sendAcc(a, profOf(a)); msg(a, 'mpFriendAdded');
}
function fno(a, id) { a.reqs = a.reqs.filter(x => x !== id); save(); sendAcc(a, profOf(a)); }
function fdel(a, id) {
  a.friends = a.friends.filter(x => x !== id);
  const b = accs.get(id);
  if (b) { b.friends = b.friends.filter(x => x !== a.id); sendAcc(b, profOf(b)); }
  save(); sendAcc(a, profOf(a));
}
function invite(a, id) {
  const b = accs.get(id);
  if (!b || !a.friends.includes(id)) return msg(a, 'mpNoFriend');
  if (!b.ws) return msg(a, 'mpNotOnline');
  if (!a.room) enter(a, mkRoom(false, a.id));
  if (a.room.st !== 'lobby') return msg(a, 'mpBusy');
  sendAcc(b, { t: 'inv', from: { id: a.id, n: a.name }, code: a.room.code });
  msg(a, 'mpInvSent');
}

// ---------- odalar ----------
function mkRoom(pub, host) {
  let code;
  do { code = Array.from({ length: 5 }, () => CODE_CH[crypto.randomInt(CODE_CH.length)]).join(''); } while (rooms.has(code));
  const r = { code, pub, host, st: 'lobby', mem: new Map(), timer: null, tick: null, maxT: null, seed: 0 };
  rooms.set(code, r);
  return r;
}
function roomView(r) {
  return {
    t: 'room', room: {
      code: r.code, pub: r.pub, host: r.host, st: r.st, max: MAX, min: MIN,
      players: [...r.mem.values()].filter(m => !m.gone).map(m => ({ id: m.id, n: m.n }))
    }
  };
}
function bcastRoom(r, o) { for (const m of r.mem.values()) if (!m.gone) sendAcc(accs.get(m.id), o); }
function sendRoomAll(r) { bcastRoom(r, roomView(r)); }

function enter(a, r) {
  leaveRoom(a);
  r.mem.set(a.id, { id: a.id, n: a.name, alive: true, gone: false, x: 0, y: 0, s: 'broc', place: 0 });
  a.room = r;
  sendRoomAll(r);
  pushFriends(a);
  if (r.pub) {
    if (r.mem.size >= MAX) startRound(r);
    else if (r.mem.size >= MIN && !r.timer) r.timer = setTimeout(() => startRound(r), LOBBY_WAIT);
  }
}
function quick(a) {
  if (a.room) return;
  let r = null;
  for (const x of rooms.values()) if (x.pub && x.st === 'lobby' && x.mem.size < MAX) { r = x; break; }
  enter(a, r || mkRoom(true, null));
}
function create(a) { if (a.room) leaveRoom(a); enter(a, mkRoom(false, a.id)); }
function joinCode(a, c) {
  const r = rooms.get(c);
  if (!r) return msg(a, 'mpNoRoom');
  if (r.st !== 'lobby') return msg(a, 'mpBusy');
  if (r.mem.size >= MAX) return msg(a, 'mpFull');
  if (a.room === r) return;
  enter(a, r);
}
function leaveRoom(a) {
  const r = a.room;
  if (!r) return;
  a.room = null;
  const m = r.mem.get(a.id);
  if (r.st === 'play') {
    if (m) { m.gone = true; kill(r, m); }
  } else {
    r.mem.delete(a.id);
    if (r.host === a.id) r.host = r.pub ? null : ([...r.mem.keys()][0] || null);
    if (r.mem.size === 0) { clearTimeout(r.timer); rooms.delete(r.code); }
    else {
      if (r.pub && r.mem.size < MIN) { clearTimeout(r.timer); r.timer = null; }
      sendRoomAll(r);
    }
  }
  send(a.ws, { t: 'room', room: null });
  pushFriends(a);
}
function startByHost(a) {
  const r = a.room;
  if (!r || r.pub || r.host !== a.id || r.st !== 'lobby') return;
  if (r.mem.size < MIN) return msg(a, 'mpNeed2');
  startRound(r);
}

// ---------- maç ----------
function startRound(r) {
  clearTimeout(r.timer); r.timer = null;
  if (r.st !== 'lobby' || r.mem.size < MIN) return;
  r.st = 'play';
  r.seed = crypto.randomInt(1, 2147483647);
  for (const m of r.mem.values()) { m.alive = true; m.gone = false; m.place = 0; m.x = 0; m.y = 0; }
  bcastRoom(r, { t: 'start', seed: r.seed });
  r.tick = setInterval(() => snap(r), TICK);
  r.maxT = setTimeout(() => endRound(r), ROUND_MAX);
  for (const m of r.mem.values()) { const a = accs.get(m.id); if (a) pushFriends(a); }
}
function pos(a, m) {
  const r = a.room;
  if (!r || r.st !== 'play') return;
  const p = r.mem.get(a.id);
  if (!p || !p.alive) return;
  p.x = clamp(+m.x || 0, -2, 2);
  p.y = clamp(+m.y || 0, -2, 2);
  if (SKINS.has(m.s)) p.s = m.s;
}
function dieMsg(a) { const r = a.room; if (r && r.st === 'play') kill(r, r.mem.get(a.id)); }
function kill(r, m) {
  if (!m || !m.alive || r.st !== 'play') return;
  m.alive = false;
  const left = [...r.mem.values()].filter(x => x.alive).length;
  m.place = left + 1;
  sendAcc(accs.get(m.id), { t: 'place', n: m.place });
  if (left <= 1) endRound(r);
}
function snap(r) {
  const p = {}; let n = 0;
  for (const m of r.mem.values()) {
    if (m.gone) continue;
    if (m.alive) n++;
    p[m.id] = { n: m.n, x: m.x, y: m.y, s: m.s, a: m.alive ? 1 : 0 };
  }
  bcastRoom(r, { t: 'snap', p, alive: n });
}
function endRound(r) {
  if (r.st !== 'play') return;
  r.st = 'ended';
  clearInterval(r.tick); clearTimeout(r.maxT);
  for (const m of r.mem.values()) if (m.alive) { m.alive = false; m.place = 1; }
  const ranks = [...r.mem.values()].sort((x, y) => x.place - y.place).map(m => ({ id: m.id, n: m.n, place: m.place }));
  const w = ranks[0];
  bcastRoom(r, { t: 'end', win: w ? w.id : null, wn: w ? w.n : '', ranks });
  rooms.delete(r.code);
  for (const m of r.mem.values()) {
    const a = accs.get(m.id);
    if (!a) continue;
    if (a.room === r) { a.room = null; sendAcc(a, { t: 'room', room: null }); }
    pushFriends(a);
  }
  r.mem.clear();
}

setInterval(() => wss.clients.forEach(w => { if (!w.isAlive) return w.terminate(); w.isAlive = false; w.ping(); }), 30000);
console.log('Save the Bob MP sunucu :' + PORT);
