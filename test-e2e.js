'use strict';

// E2E test for Whisper v2 (raw WebSockets).
// Spawns the server on a test port with a throwaway SQLite DB, exercises the
// full protocol over raw `ws` clients, then tears everything down.
//
// Covers: auth-at-upgrade, hello-ok, presence, group rooms, message routing,
// typing, DMs, DM isolation, history-on-join, resumable reconnect (STEP 8),
// rate limiting (STEP 12), persistence (STEP 9), fanout bus delivery
// (STEP 13), and presence online/offline (STEP 14).

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
const http = require('http');
const WebSocket = require('ws');

const PORT = 3123;
const BASE = 'http://localhost:' + PORT;
const WS_BASE = 'ws://localhost:' + PORT;

// Unique names so the test is deterministic even if a browser tab is open.
const RUN = Date.now().toString(36);
const ALICE = 'Alice-' + RUN;
const BOB = 'Bob-' + RUN;
const CAROL = 'Carol-' + RUN;

let failures = 0;
function check(name, cond) {
  console.log((cond ? 'PASS' : 'FAIL') + ' - ' + name);
  if (!cond) failures++;
}

// ---------- HTTP helpers ----------
function postJson(urlPath, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      BASE + urlPath,
      { method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } },
      (res) => {
        let buf = '';
        res.on('data', (c) => (buf += c));
        res.on('end', () => {
          try { resolve({ status: res.statusCode, body: JSON.parse(buf) }); }
          catch (e) { reject(e); }
        });
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function createSession(name) {
  const r = await postJson('/api/session', { name });
  return r.body;
}

// ---------- WebSocket client wrapper ----------
class Client {
  constructor(token) {
    this.token = token;
    this.ws = null;
    this.queue = [];
    this.waiters = [];
  }
  connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(WS_BASE + '/ws?token=' + encodeURIComponent(this.token));
      this.ws.on('open', () => resolve(this));
      this.ws.on('error', reject);
      this.ws.on('message', (data) => {
        let env;
        try { env = JSON.parse(data.toString()); } catch { return; }
        const waiter = this.waiters.find((w) => w.pred(env));
        if (waiter) {
          this.waiters.splice(this.waiters.indexOf(waiter), 1);
          waiter.resolve(env);
        } else {
          this.queue.push(env);
        }
      });
    });
  }
  send(type, payload) {
    this.ws.send(JSON.stringify({ type, payload: payload || {} }));
  }
  waitFor(pred, timeoutMs = 4000) {
    const idx = this.queue.findIndex(pred);
    if (idx !== -1) return Promise.resolve(this.queue.splice(idx, 1)[0]);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('waitFor timeout: ' + pred.toString())), timeoutMs);
      this.waiters.push({ pred, resolve: (env) => { clearTimeout(t); resolve(env); } });
    });
  }
  close() {
    if (this.ws) this.ws.close();
  }
}

// Wait for the server to print its ready line.
function startServer(dbPath) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.join(__dirname, 'server.js')], {
      env: { ...process.env, PORT: String(PORT), DB_PATH: dbPath },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let out = '';
    const onData = (c) => {
      out += c.toString();
      if (out.includes('running at')) resolve(child);
    };
    child.stdout.on('data', onData);
    child.stderr.on('data', onData);
    child.on('error', reject);
    setTimeout(() => reject(new Error('server did not start in time: ' + out)), 8000);
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Global timeout so the test can never hang indefinitely.
const globalTimeout = setTimeout(() => {
  console.error('TIMEOUT - test did not finish in 30s');
  process.exit(1);
}, 30000);

(async () => {
  // Throwaway DB in a temp dir.
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'whisper-e2e-'));
  const dbPath = path.join(tmpDir, 'whisper.db');

  const server = await startServer(dbPath);
  console.log('server started (pid ' + server.pid + ')');

  // ---------- 1. Auth at upgrade (STEP 4) ----------
  const badConnect = new Promise((resolve) => {
    const ws = new WebSocket(WS_BASE + '/ws?token=not-a-real-token');
    ws.on('open', () => resolve('open'));
    ws.on('error', () => resolve('error'));
    ws.on('close', (code) => resolve('close:' + code));
  });
  const badResult = await badConnect;
  check('bad token rejected at upgrade', badResult !== 'open');

  // ---------- 2. Sessions + hello-ok ----------
  const sa = await createSession(ALICE);
  const sb = await createSession(BOB);
  check('session created for alice', sa.ok === true && !!sa.token && sa.name === ALICE);
  check('session created for bob', sb.ok === true && !!sb.token);

  const alice = await new Client(sa.token).connect();
  const bob = await new Client(sb.token).connect();

  const helloA = await alice.waitFor((e) => e.type === 'hello-ok');
  check('alice hello-ok', helloA.payload.name === ALICE && Array.isArray(helloA.payload.online));

  // ---------- 3. Presence ----------
  alice.send('list');
  const listA = await alice.waitFor((e) => e.type === 'list');
  check('presence shows both users', listA.payload.online.includes(ALICE) && listA.payload.online.includes(BOB));

  // ---------- 4. Create + join group room ----------
  alice.send('create-room', { name: 'General' });
  const created = await alice.waitFor((e) => e.type === 'room-created');
  check('room created', created.payload.room.name === 'General');
  const roomId = created.payload.room.id;

  bob.send('join-room', { roomId });
  const joined = await bob.waitFor((e) => e.type === 'joined');
  check('bob joined room', joined.payload.room.id === roomId);

  // ---------- 5. Group message routing ----------
  const msgP = bob.waitFor((e) => e.type === 'message' && e.payload.msg.roomId === roomId);
  alice.send('message', { roomId, text: 'Hello Bob!' });
  const msg = await msgP;
  check('group message delivered', msg.payload.msg.text === 'Hello Bob!' && msg.payload.msg.sender === ALICE);

  // ---------- 6. Typing indicator ----------
  const typingP = bob.waitFor((e) => e.type === 'typing' && e.payload.roomId === roomId);
  alice.send('typing', { roomId, typing: true });
  const t = await typingP;
  check('typing indicator delivered', t.payload.name === ALICE && t.payload.typing === true);

  // ---------- 7. DM open + both directions ----------
  const dmOpenedAlice = alice.waitFor((e) => e.type === 'dm-opened');
  bob.send('dm-open', { to: ALICE });
  const dmRes = await bob.waitFor((e) => e.type === 'dm-opened');
  check('dm opened', dmRes.payload.peer === ALICE);
  const dmInfo = await dmOpenedAlice;
  check('dm-opened notified other side', dmInfo.payload.peer === BOB);
  const dmRoomId = dmRes.payload.roomId;

  const dmMsgBob = bob.waitFor((e) => e.type === 'message' && e.payload.msg.roomId === dmRoomId);
  alice.send('message', { roomId: dmRoomId, text: 'psst, hi' });
  const dm1 = await dmMsgBob;
  check('dm message alice->bob', dm1.payload.msg.text === 'psst, hi' && dm1.payload.msg.sender === ALICE);

  // Match on the specific text: the sender also receives its own echo, so
  // Alice's queue already holds her "psst, hi" DM message.
  const dmMsgAlice = alice.waitFor((e) => e.type === 'message' && e.payload.msg.roomId === dmRoomId && e.payload.msg.text === 'psst, hello');
  bob.send('message', { roomId: dmRoomId, text: 'psst, hello' });
  const dm2 = await dmMsgAlice;
  check('dm message bob->alice', dm2.payload.msg.text === 'psst, hello' && dm2.payload.msg.sender === BOB);

  // ---------- 8. DM isolation ----------
  await sleep(150);
  const groupGotDm = alice.queue.some((e) => e.type === 'message' && e.payload.msg.roomId === roomId && e.payload.msg.text === 'psst, hi');
  check('dm not leaked into group room', groupGotDm === false);

  // ---------- 9. Room list shows DM with correct peer ----------
  alice.send('list');
  const listA2 = await alice.waitFor((e) => e.type === 'list');
  const dmRoomA = listA2.payload.rooms.find((r) => r.id === dmRoomId);
  check('alice sees dm with peer Bob', dmRoomA && dmRoomA.kind === 'dm' && dmRoomA.peer === BOB);

  // ---------- 10. History on join (STEP 10) ----------
  // Seed a few messages, then a fresh client joins and should get them.
  alice.send('message', { roomId, text: 'hist-1' });
  alice.send('message', { roomId, text: 'hist-2' });
  await sleep(100);
  const sc = await createSession(CAROL);
  const carol = await new Client(sc.token).connect();
  carol.send('join-room', { roomId });
  const hist = await carol.waitFor((e) => e.type === 'history' && e.room === roomId);
  const histTexts = hist.payload.messages.map((m) => m.text);
  check('history-on-join includes seeded messages', histTexts.includes('hist-1') && histTexts.includes('hist-2'));
  check('history is oldest->newest', histTexts.indexOf('hist-1') < histTexts.indexOf('hist-2'));

  // ---------- 10b. Access control: Carol is not in Alice/Bob's DM ----------
  carol.send('message', { roomId: dmRoomId, text: 'intrude' });
  const postErr = await carol.waitFor((e) => e.type === 'error');
  check('non-member cannot post to DM', /Not a member/.test(postErr.payload.error));
  carol.send('history', { roomId: dmRoomId });
  const histErr = await carol.waitFor((e) => e.type === 'error' && e !== postErr);
  check('non-member cannot read DM history', /Not a member/.test(histErr.payload.error));
  carol.send('resume', { rooms: [dmRoomId], lastSeen: {} });
  await carol.waitFor((e) => e.type === 'resumed');
  const leaked = carol.queue.some((e) => e.type === 'message' && e.payload.msg.roomId === dmRoomId);
  check('resume does not join a DM you are not in', !leaked);

  // ---------- 11. Resumable reconnect (STEP 8) ----------
  // Bob's last-seen id in the group room:
  const bobLastSeen = msg.payload.msg.id; // 'Hello Bob!'
  bob.close();
  await sleep(150);
  // Alice sends two more while Bob is away.
  alice.send('message', { roomId, text: 'away-1' });
  alice.send('message', { roomId, text: 'away-2' });
  alice.send('message', { roomId: dmRoomId, text: 'dm-away' });
  await sleep(150);

  // Bob reconnects with the same session token and resumes.
  const bob2 = await new Client(sb.token).connect();
  bob2.send('resume', { rooms: [roomId, dmRoomId], lastSeen: { [roomId]: bobLastSeen, [dmRoomId]: dm2.payload.msg.id } });
  const resumed = await bob2.waitFor((e) => e.type === 'resumed');
  check('resume reports replayed count', resumed.payload.count >= 2);
  // Verify the replayed messages are the ones sent while away.
  const replayed = bob2.queue.filter((e) => e.type === 'message' && e.payload.msg.roomId === roomId).map((e) => e.payload.msg.text);
  check('resume replayed missed messages', replayed.includes('away-1') && replayed.includes('away-2'));
  const replayedDm = bob2.queue.filter((e) => e.type === 'message' && e.payload.msg.roomId === dmRoomId).map((e) => e.payload.msg.text);
  check('DM party can resume into their own DM', replayedDm.length === 1 && replayedDm[0] === 'dm-away');

  // ---------- 12. Rate limiting (STEP 12) ----------
  const rl = await new Client(sa.token).connect();
  let sawRateLimited = false;
  for (let i = 0; i < 30; i++) {
    rl.send('message', { roomId, text: 'flood-' + i });
  }
  try {
    const env = await rl.waitFor((e) => e.type === 'rate-limited', 3000);
    sawRateLimited = !!env;
  } catch { /* no rate-limited within window */ }
  check('rate limiter engages under flood', sawRateLimited === true);
  rl.close();

  // ---------- 13. Persistence (STEP 9) ----------
  // Open the DB read-only and confirm messages were written.
  const { DatabaseSync } = require('node:sqlite');
  const db = new DatabaseSync(dbPath, { readOnly: true });
  const rows = db.prepare('SELECT COUNT(*) AS n FROM messages WHERE room_id = ?').get(roomId);
  db.close();
  check('messages persisted to SQLite', rows.n >= 5);

  // ---------- 14. STEP 13: fanout bus delivers to a late-joining member ----------
  // Carol is already a member of the group room. A message published to the
  // room must be fanned out to her via the bus (MemoryBus in this run).
  const carolFanout = carol.waitFor((e) => e.type === 'message' && e.payload.msg.roomId === roomId && e.payload.msg.text === 'fanout-check');
  alice.send('message', { roomId, text: 'fanout-check' });
  const fanoutMsg = await carolFanout;
  check('fanout bus delivers to late-joining member', fanoutMsg.payload.msg.text === 'fanout-check' && fanoutMsg.payload.msg.sender === ALICE);

  // ---------- 15. STEP 14: presence reflects online/offline ----------
  // While Carol is connected, her name must appear in the online list.
  alice.send('list');
  const listWithCarol = await alice.waitFor((e) => e.type === 'list');
  check('presence includes connected user', listWithCarol.payload.online.includes(CAROL));

  // Disconnect Carol; after a short settle her name must drop from presence.
  carol.close();
  await sleep(250);
  alice.send('list');
  const listWithoutCarol = await alice.waitFor((e) => e.type === 'list');
  check('presence drops disconnected user', listWithoutCarol.payload.online.includes(ALICE) && !listWithoutCarol.payload.online.includes(CAROL));

  // ---------- teardown ----------
  alice.close(); bob2.close();
  await sleep(100);
  await new Promise((resolve) => {
    server.once('exit', resolve);
    server.kill();
    setTimeout(resolve, 1500); // safety net if exit event is slow
  });
  // Best-effort cleanup: on Windows the SQLite WAL file may still be locked
  // for a moment after the child exits, so don't let a failed rm fail the run.
  try { fs.rmSync(tmpDir, { recursive: true, force: true }); }
  catch (e) { console.log('note: could not remove temp dir (' + e.code + ')'); }

  clearTimeout(globalTimeout);
  console.log(failures === 0 ? '\nALL TESTS PASSED' : `\n${failures} TEST(S) FAILED`);
  process.exit(failures === 0 ? 0 : 1);
})().catch((e) => {
  clearTimeout(globalTimeout);
  console.error('ERROR', e);
  process.exit(1);
});
