'use strict';

// Whisper v2 — real-time instant messaging on RAW WebSockets (no Socket.IO).
// Built step-by-step per instant-messaging-app-build-guide.txt.
//
//   STEP 1  WebSocket server (ws) — bare connection, echo-capable
//   STEP 2  Message envelope { id, type, room, payload, ts } + ULID ids
//   STEP 3  Message routing by `type` + server-side room registry
//   STEP 4  Auth validated during the HTTP upgrade (token), reject outright
//   STEP 5  Native ping/pong heartbeats, terminate after missed pongs
//   STEP 6  Origin validated in the upgrade handler
//   STEP 7  (client) exponential-backoff reconnection
//   STEP 8  (client+server) resumable reconnect: replay after lastSeenId
//   STEP 9  SQLite message store (node:sqlite), index (room_id, ts)
//   STEP 10 History on join (last N) + cursor pagination
//   STEP 11 Backpressure via bufferedAmount (skip typing, drop slow clients)
//   STEP 12 Per-socket token-bucket rate limiting
//   STEP 13 Pub/sub fanout bus (pluggable: in-memory default, Redis optional)
//   STEP 14 Presence in a shared store (in-memory default, Redis TTL optional)
//   STEP 15 Fanout via Redis Streams (optional, FANOUT_MODE=streams)
//   STEP 16 WebTransport (HTTP/3/QUIC) — documented future upgrade path

const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');
const { ulid } = require('./lib/ulid');
const { TokenBucket } = require('./lib/rate-limit');
const { Store, defaultDbPath } = require('./lib/store');
const { createBus } = require('./lib/bus');
const { createPresence } = require('./lib/presence');

const app = express();
const server = http.createServer(app);
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---------- config ----------
const PORT = process.env.PORT || 3000;
const HISTORY_LIMIT = 50;          // STEP 10: last N on join
const PAGE_LIMIT = 50;             // STEP 10: pagination page size
const PING_INTERVAL_MS = 25000;    // STEP 5: ping cadence
const MAX_MISSED_PONGS = 3;        // STEP 5: terminate after this many missed
const SOFT_BUFFER_BYTES = 1024 * 1024;   // STEP 11: skip non-critical above this
const HARD_BUFFER_BYTES = 4 * 1024 * 1024; // STEP 11: terminate above this
const RATE_CAPACITY = 20;          // STEP 12: burst
const RATE_PER_SEC = 10;           // STEP 12: sustained msgs/sec
const ALLOWED_ORIGINS = new Set([  // STEP 6
  'http://localhost:' + PORT,
  'http://127.0.0.1:' + PORT,
  ...(process.env.ALLOWED_ORIGINS ? process.env.ALLOWED_ORIGINS.split(',').map((s) => s.trim()) : []),
]);
// STEP 13/14/15: horizontal-scaling fanout + presence.
// Default is in-memory (single process, zero external deps). Set
// FANOUT_MODE=redis (pub/sub) or FANOUT_MODE=streams (durable) plus REDIS_URL
// to scale across multiple server instances.
const FANOUT_MODE = String(process.env.FANOUT_MODE || 'memory').toLowerCase();
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const PRESENCE_TTL_MS = 30000;   // STEP 14: presence TTL (renewed on heartbeat)

// ---------- persistence (STEP 9) ----------
const store = new Store(process.env.DB_PATH || defaultDbPath);

// ---------- fanout bus + presence (STEP 13/14/15) ----------
const bus = createBus({ mode: FANOUT_MODE, redisUrl: REDIS_URL });
const presence = createPresence({ mode: FANOUT_MODE, redisUrl: REDIS_URL, ttlMs: PRESENCE_TTL_MS });

// ---------- in-memory state (STEP 3) ----------
const sessions = new Map();   // token -> { id, name }
const sockets = new Map();    // ws -> { session, name, rooms:Set, bucket, isAlive, missedPongs }
const rooms = new Map();      // roomId -> { id, name, kind, members:Set, names?:[] }

const LOBBY = 'lobby';
rooms.set(LOBBY, { id: LOBBY, name: 'Lobby', kind: 'lobby', members: new Set() });

function dmRoomKey(a, b) {
  return 'dm-' + [a, b].sort().join('--');
}
function ensureDmRoom(a, b, nameA, nameB) {
  const id = dmRoomKey(a, b);
  if (!rooms.has(id)) rooms.set(id, { id, name: 'DM', kind: 'dm', members: new Set(), names: [nameA, nameB] });
  return id;
}
// DMs are private: only the two parties (recorded in room.names) may join or
// resume them. Group rooms are open to anyone.
function mayJoin(st, room) {
  return room.kind !== 'dm' || (room.names || []).includes(st.name);
}
function onlineNames() {
  // STEP 14: presence comes from the shared store (in-memory or Redis).
  return presence.online();
}
function roomListFor(st) {
  // DMs are private: a user only sees their own DMs (rooms they are a member
  // of). Group rooms are visible to everyone. This prevents leaking other
  // users' DM room IDs / peers into the room list.
  return [...rooms.values()]
    .filter((r) => r.kind !== 'lobby' && (r.kind !== 'dm' || st.rooms.has(r.id)))
    .map((r) => {
      const base = { id: r.id, name: r.name, kind: r.kind, members: r.members.size };
      if (r.kind === 'dm') base.peer = (r.names || []).find((n) => n !== st.name) || null;
      return base;
    });
}
function broadcastRooms() {
  for (const st of sockets.values()) send(st, { type: 'rooms', payload: { rooms: roomListFor(st) } });
}
function broadcastPresence() {
  const online = onlineNames();
  for (const st of sockets.values()) send(st, { type: 'presence', payload: { online } });
}
function findSocketByName(name) {
  for (const st of sockets.values()) if (st.name === name) return st;
  return null;
}

// ---------- wire helpers (STEP 2) ----------
function envelope(type, room, payload) {
  return { id: ulid(), type, room: room || null, payload: payload || {}, ts: Date.now() };
}
// STEP 11: backpressure-aware send. Returns false if the socket is gone.
function send(st, env, critical = true) {
  const ws = st.ws;
  if (ws.readyState !== WebSocket.OPEN) return false;
  if (!critical && ws.bufferedAmount > SOFT_BUFFER_BYTES) return true; // drop non-critical
  if (ws.bufferedAmount > HARD_BUFFER_BYTES) { ws.terminate(); return false; }
  ws.send(JSON.stringify(env));
  return true;
}
// STEP 13: room fanout goes through the bus. Each instance subscribes a
// handler per room it has local members in; the handler delivers to those
// local sockets (with backpressure). In a single process the MemoryBus
// dispatches synchronously, so behavior is identical to direct delivery.
const roomHandlers = new Map(); // roomId -> handler
function ensureRoomHandler(roomId) {
  if (roomHandlers.has(roomId)) return;
  const handler = (value) => {
    const env = value && value.env ? value.env : value;
    const critical = value && typeof value.critical === 'boolean' ? value.critical : true;
    const room = rooms.get(roomId);
    if (!room) return;
    for (const ws of room.members) {
      const st = sockets.get(ws);
      if (st) send(st, env, critical);
    }
  };
  roomHandlers.set(roomId, handler);
  Promise.resolve(bus.subscribe(roomId, handler)).catch((e) => console.error('bus subscribe error', e));
}
function sendToRoom(roomId, env, critical = true) {
  ensureRoomHandler(roomId);
  // STEP 15: wait for the room's subscription to be established before
  // publishing. RedisStreamBus.subscribe() records the stream's current last
  // entry id as its read position; publishing first would land the message
  // before that position and this instance would never deliver it.
  Promise.resolve(bus.ready(roomId))
    .then(() => bus.publish(roomId, { env, critical }))
    .catch((e) => console.error('bus publish error', e));
}

// ---------- session auth (STEP 4) ----------
// Create a session over HTTP; the returned token is validated at the WS upgrade.
app.post('/api/session', (req, res) => {
  const name = String((req.body && req.body.name) || '').trim().slice(0, 30);
  if (!name) return res.status(400).json({ ok: false, error: 'Name required' });
  const token = crypto.randomBytes(24).toString('hex');
  const id = ulid();
  sessions.set(token, { id, name });
  store.saveSession(id, name);
  res.json({ ok: true, token, sessionId: id, name });
});

// ---------- WebSocket server (STEP 1) ----------
const wss = new WebSocketServer({ noServer: true });

// STEP 6: Origin check + STEP 4: token auth, both during the upgrade.
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname !== '/ws') { socket.destroy(); return; }

  // STEP 6: Origin check. Browsers always send Origin; non-browser clients
  // (e.g. the e2e test) send none, which we allow.
  const origin = req.headers.origin;
  if (origin && !ALLOWED_ORIGINS.has(origin)) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
    socket.destroy();
    return;
  }

  const token = url.searchParams.get('token');
  const session = token && sessions.get(token);
  if (!session) {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req, session);
  });
});

wss.on('connection', (ws, req, session) => {
  const st = {
    ws,
    session,
    name: session.name,
    rooms: new Set([LOBBY]),
    bucket: new TokenBucket(RATE_CAPACITY, RATE_PER_SEC), // STEP 12
    isAlive: true,
    missedPongs: 0,
  };
  sockets.set(ws, st);
  rooms.get(LOBBY).members.add(ws);
  ensureRoomHandler(LOBBY);
  presence.touch(st.name); // STEP 14: mark online (renewed on heartbeat)

  // STEP 5: track pong responses
  ws.on('pong', () => { st.isAlive = true; st.missedPongs = 0; });

  // Greet with current state (rooms + presence)
  send(st, envelope('hello-ok', null, { name: st.name, rooms: roomListFor(st), online: onlineNames() }));
  sendToRoom(LOBBY, envelope('system', LOBBY, { text: `${st.name} joined the lobby` }));
  broadcastPresence();

  ws.on('message', (data) => onMessage(st, data));
  ws.on('close', () => onDisconnect(st));
  ws.on('error', () => onDisconnect(st));
});

// ---------- routing (STEP 3) ----------
function onMessage(st, data) {
  let env;
  try {
    env = JSON.parse(data.toString());
  } catch {
    return send(st, envelope('error', null, { error: 'Invalid JSON' }));
  }
  if (!env || typeof env.type !== 'string') {
    return send(st, envelope('error', null, { error: 'Missing type' }));
  }

  // STEP 12: rate limit (resume is exempt)
  if (env.type !== 'resume' && !st.bucket.take()) {
    return send(st, envelope('rate-limited', null, { limit: RATE_PER_SEC }));
  }

  const p = env.payload || {};
  switch (env.type) {
    case 'resume':      return handleResume(st, p);
    case 'list':        return handleList(st);
    case 'create-room': return handleCreateRoom(st, p);
    case 'join-room':   return handleJoinRoom(st, p);
    case 'leave-room':  return handleLeaveRoom(st, p);
    case 'dm-open':     return handleDmOpen(st, p);
    case 'message':     return handleMessage(st, p);
    case 'typing':      return handleTyping(st, p);
    case 'history':     return handleHistory(st, p);
    default:
      return send(st, envelope('error', null, { error: 'Unknown type: ' + env.type }));
  }
}

function handleList(st) {
  send(st, envelope('list', null, { rooms: roomListFor(st), online: onlineNames() }));
}

// STEP 8: replay messages after lastSeenId so reconnect is resumable.
// Client sends the rooms it was in + per-room last-seen message id.
function handleResume(st, p) {
  const roomIds = Array.isArray(p.rooms) ? p.rooms : [...st.rooms];
  const lastSeen = p.lastSeen && typeof p.lastSeen === 'object' ? p.lastSeen : {};
  const replayed = [];
  for (const roomId of roomIds) {
    if (roomId === LOBBY) continue;
    const room = rooms.get(roomId);
    if (!room || !mayJoin(st, room)) continue;
    // Re-attach this socket to the room (membership was lost on disconnect).
    if (!st.rooms.has(roomId)) {
      st.rooms.add(roomId);
      room.members.add(st.ws);
    }
    const afterId = String(lastSeen[roomId] || '');
    const msgs = store.messagesAfter(roomId, afterId, 200);
    for (const m of msgs) {
      replayed.push(m);
      send(st, envelope('message', roomId, { msg: m }));
    }
  }
  broadcastRooms();
  send(st, envelope('resumed', null, { count: replayed.length }));
}

function handleCreateRoom(st, p) {
  const name = String(p.name || '').trim().slice(0, 30);
  if (!name) return send(st, envelope('error', null, { error: 'Room name required' }));
  const id = 'room-' + ulid().toLowerCase();
  rooms.set(id, { id, name, kind: 'group', members: new Set([st.ws]) });
  st.rooms.add(id);
  broadcastRooms();
  send(st, envelope('room-created', id, { room: { id, name, members: 1 } }));
}

function handleJoinRoom(st, p) {
  const room = rooms.get(p.roomId);
  if (!room) return send(st, envelope('error', null, { error: 'Room not found' }));
  // Stops a user from joining someone else's DM by guessing its ID.
  if (!mayJoin(st, room)) {
    return send(st, envelope('error', null, { error: 'Not a member of this DM' }));
  }
  if (!st.rooms.has(room.id)) {
    st.rooms.add(room.id);
    room.members.add(st.ws);
    // STEP 10: send history on join (batched, oldest -> newest)
    const history = store.recentMessages(room.id, HISTORY_LIMIT);
    send(st, envelope('history', room.id, { messages: history, hasMore: history.length === HISTORY_LIMIT }));
    send(st, envelope('joined', room.id, { room: { id: room.id, name: room.name, kind: room.kind } }));
  }
}

function handleLeaveRoom(st, p) {
  const room = rooms.get(p.roomId);
  if (!room || room.kind === 'lobby') return;
  st.rooms.delete(room.id);
  room.members.delete(st.ws);
  if (room.members.size === 0) {
    rooms.delete(room.id);
    broadcastRooms();
  }
}

function handleDmOpen(st, p) {
  const to = String(p.to || '').trim();
  if (!to) return send(st, envelope('error', null, { error: 'Missing user' }));
  const target = findSocketByName(to);
  if (!target) return send(st, envelope('error', null, { error: 'User not online' }));
  if (target.session.id === st.session.id) {
    return send(st, envelope('error', null, { error: 'Cannot DM yourself' }));
  }
  const roomId = ensureDmRoom(st.session.id, target.session.id, st.name, to);
  const room = rooms.get(roomId);
  room.members.add(st.ws);
  room.members.add(target.ws);
  if (!st.rooms.has(roomId)) st.rooms.add(roomId);
  if (!target.rooms.has(roomId)) target.rooms.add(roomId);
  send(st, envelope('dm-opened', roomId, { roomId, peer: to }));
  send(target, envelope('dm-opened', roomId, { roomId, peer: st.name }));
  broadcastRooms();
}

function handleMessage(st, p) {
  const room = rooms.get(p.roomId);
  if (!room) return send(st, envelope('error', null, { error: 'Room not found' }));
  if (!st.rooms.has(room.id)) return send(st, envelope('error', null, { error: 'Not a member of this room' }));
  const text = String(p.text || '').trim().slice(0, 2000);
  if (!text) return;
  const msg = { id: ulid(), roomId: room.id, sender: st.name, text, ts: Date.now() };
  store.saveMessage(msg); // STEP 9
  sendToRoom(room.id, envelope('message', room.id, { msg }));
}

function handleTyping(st, p) {
  const room = rooms.get(p.roomId);
  if (!room || !st.rooms.has(room.id)) return;
  // STEP 11: typing is non-critical — dropped under backpressure
  sendToRoom(room.id, envelope('typing', room.id, { roomId: room.id, name: st.name, typing: !!p.typing }), false);
}

// STEP 10: cursor pagination for "load older messages"
function handleHistory(st, p) {
  const room = rooms.get(p.roomId);
  if (!room) return send(st, envelope('error', null, { error: 'Room not found' }));
  if (!st.rooms.has(room.id)) return send(st, envelope('error', null, { error: 'Not a member of this room' }));
  const beforeId = String(p.beforeId || '');
  const msgs = beforeId
    ? store.messagesBefore(room.id, beforeId, PAGE_LIMIT)
    : store.recentMessages(room.id, PAGE_LIMIT);
  send(st, envelope('history', room.id, { messages: msgs, hasMore: msgs.length === PAGE_LIMIT }));
}

// ---------- disconnect ----------
function onDisconnect(st) {
  if (!sockets.has(st.ws)) return;
  sockets.delete(st.ws);
  for (const roomId of [...st.rooms]) {
    const room = rooms.get(roomId);
    if (room) {
      room.members.delete(st.ws);
      if (room.members.size === 0 && room.kind !== 'lobby') rooms.delete(roomId);
    }
  }
  st.rooms.clear();
  // STEP 14: mark offline only if no other socket for this name remains
  // (a user may hold multiple sockets, e.g. two browser tabs).
  const stillOnline = [...sockets.values()].some((s) => s.name === st.name);
  if (!stillOnline) presence.remove(st.name);
  broadcastPresence();
  broadcastRooms();
  sendToRoom(LOBBY, envelope('system', LOBBY, { text: `${st.name} left` }));
}

// ---------- heartbeats (STEP 5) ----------
setInterval(() => {
  for (const st of sockets.values()) {
    const ws = st.ws;
    if (ws.readyState !== WebSocket.OPEN) continue;
    if (!st.isAlive) {
      st.missedPongs += 1;
      if (st.missedPongs >= MAX_MISSED_PONGS) { ws.terminate(); continue; }
    }
    st.isAlive = false;
    ws.ping();
    presence.touch(st.name); // STEP 14: renew presence TTL on heartbeat
  }
}, PING_INTERVAL_MS).unref();

// ---------- startup / shutdown ----------
async function start() {
  // STEP 13/14/15: connect the fanout bus and presence store before serving.
  await bus.connect();
  await presence.connect();
  server.listen(PORT, () => {
    console.log(`Whisper v2 (raw WebSockets) running at http://localhost:${PORT}`);
    console.log(`  fanout: ${FANOUT_MODE} | presence: ${FANOUT_MODE}`);
  });
}

async function shutdown(signal) {
  console.log(`\n${signal} received, shutting down...`);
  try {
    for (const ws of sockets.keys()) {
      if (ws.readyState === WebSocket.OPEN) ws.close(1001, 'server shutting down');
    }
    await bus.close();
    await presence.close();
    store.close();
  } catch (e) {
    console.error('error during shutdown', e);
  }
  server.close(() => process.exit(0));
  // Safety: force-exit if connections linger.
  setTimeout(() => process.exit(0), 1500).unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

start().catch((e) => {
  console.error('failed to start', e);
  process.exit(1);
});
