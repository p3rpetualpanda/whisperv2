# Whisper v2

A real-time instant messaging web app built on **raw WebSockets** (the `ws` library) instead of Socket.IO. Browser-based, no build step — just `npm install` and `npm start`.

This is a ground-up rebuild of the original `whisper` app, following the 16-step build guide in `instant-messaging-app-build-guide.txt`. It keeps the same UX (display-name join, group rooms, 1-on-1 DMs, typing, presence) but replaces the Socket.IO transport with a hand-rolled WebSocket protocol and adds the reliability features the guide calls out: auth at upgrade, origin checking, heartbeats, rate limiting, backpressure, SQLite persistence, resumable reconnection, and history pagination.

## Features

- **Join with a display name** — no accounts, no passwords (session token issued over HTTP)
- **Group rooms** — create named rooms and chat with everyone inside
- **1-on-1 DMs** — click any online user in the sidebar to open a private chat
- **Typing indicators** — see when someone is typing in the current room
- **Online presence** — live list of who's online, with join/leave notices in the lobby
- **Resumable reconnection** — exponential backoff + jitter; on reconnect the client replays messages it missed per room (via `lastSeen` cursors)
- **Message history** — persisted to SQLite; recent history loads on join, with "load older" pagination
- **Rate limiting** — token-bucket throttle per connection to curb spam
- **Backpressure** — slow consumers are dropped/terminated based on socket buffer size
- **Horizontal scaling (pluggable)** — fan-out and presence are abstracted behind a bus/presence interface with a zero-dependency in-memory default; point it at Redis (Pub/Sub or Streams) to run multiple server instances

## Run

```bash
npm install
npm start
```

Then open http://localhost:3000 in a browser. Open a second tab (or another browser) and join with a different name to test DMs and group chat.

Set a custom port with `PORT=8080 npm start`.

### Horizontal scaling (optional)

By default the app runs as a single process with an in-memory fan-out bus and presence store — zero extra dependencies. To run multiple server instances behind a load balancer, switch the fan-out and presence layers to Redis:

```bash
# Redis Pub/Sub fan-out (guide step 13) + Redis presence (step 14)
FANOUT_MODE=redis REDIS_URL=redis://localhost:6379 npm start

# Fan-out via Redis Streams (guide step 15)
FANOUT_MODE=streams REDIS_URL=redis://localhost:6379 npm start
```

The `redis` npm package (node-redis v4 or later) is only required when `FANOUT_MODE` is `redis` or `streams` (`npm i redis`). The in-memory default keeps local dev dependency-free, per the build guide's advice not to add a Redis dependency until you've measured that one process can't handle the load.

### Tests

An end-to-end test (two simulated clients over the real WebSocket protocol) covers auth-at-upgrade, presence, group rooms, DMs, DM access control, typing, history-on-join, resumable replay, rate limiting, and SQLite persistence. `test-bus.js` checks the Redis Streams bus against a fake Redis client (two instances must both receive every message); the Redis modes have not been run against a real Redis server. It starts its own server on a scratch port with a temp database, so no manual setup is needed:

```bash
npm test
```

## Architecture

```
whisperv2/
├── server.js            # Express + raw WebSocket server (ws), protocol + state
├── lib/
│   ├── ulid.js          # ULID generator (sortable 26-char message IDs)
│   ├── rate-limit.js    # Token-bucket rate limiter
│   ├── store.js         # SQLite persistence (node:sqlite, WAL)
│   ├── bus.js           # Fan-out bus: MemoryBus / RedisBus / RedisStreamBus (steps 13, 15)
│   └── presence.js      # Presence store: MemoryPresence / RedisPresence (step 14)
├── public/
│   ├── index.html       # App shell (name prompt, sidebar, chat pane, room modal)
│   ├── style.css        # Dark two-pane chat UI
│   └── app.js           # Client: raw WS, reconnection, resume, history, DMs, typing
├── test-e2e.js          # End-to-end test (spawns the server, drives two clients)
└── package.json
```

### Transport

- **Server** — `ws` `WebSocketServer` in `noServer` mode, attached to the HTTP `upgrade` event. The upgrade handler enforces the auth token and origin check before the socket is accepted.
- **Client** — a plain `WebSocket` to `ws(s)://host/ws?token=...`. No Socket.IO client, no extra script tags.
- **Framing** — every message is a JSON **envelope**: `{ id, type, room, payload, ts }`. `id` is a ULID so messages are sortable and can be used as resume cursors.

### Protocol

- **Client → server**: `resume`, `list`, `create-room`, `join-room`, `leave-room`, `dm-open`, `message`, `typing`, `history`
- **Server → client**: `hello-ok`, `system`, `presence`, `rooms`, `message`, `resumed`, `room-created`, `joined`, `dm-opened`, `typing`, `history`, `error`, `rate-limited`

### Server model

- **Sessions** — `sessionId → { name }` map. A session token is minted by `POST /api/session` and validated during the WebSocket upgrade.
- **Rooms** — three kinds:
  - `lobby` — everyone is in it by default; shows join/leave system messages
  - `group` — created via `create-room`; anyone can join from the sidebar
  - `dm` — auto-created per user pair (keyed by sorted session IDs); both parties are added when either opens it
- **Persistence** — messages are written to SQLite (`data/whisper.db`, WAL mode) with an index on `(room_id, ts)`. History is served newest-first with cursor pagination.

### Horizontal scaling (guide steps 13–15)

Room fan-out and online presence are abstracted behind two small interfaces so the transport can be swapped without touching the protocol:

- **`lib/bus.js`** — a per-room pub/sub bus. `MemoryBus` (default) dispatches in-process; `RedisBus` uses Redis Pub/Sub (one channel per room); `RedisStreamBus` uses Redis Streams with plain `XREAD` (no consumer groups, so every instance reads every entry), capped with `MAXLEN ~ 1000`, on a separate blocking-read connection. A restarted instance starts from the newest entry; users catch up from SQLite via `resume`.
- **`lib/presence.js`** — a TTL presence store. `MemoryPresence` (default) tracks last-seen timestamps in-process; `RedisPresence` writes one key per user with a ~30s TTL renewed on each heartbeat, so any instance can answer "who's online".

Both are selected via `FANOUT_MODE` (`memory` | `redis` | `streams`) and `REDIS_URL`. The `redis` package is lazy-required only when a Redis-backed mode is chosen, so the default path stays zero-dependency. This is the guide's "only if needed" layer: it's wired up and checked offline, but off by default.

### WebTransport (guide step 16)

WebTransport (HTTP/3/QUIC, multiple independent streams, no head-of-line blocking) is the documented future upgrade path for the transport. It is **not implemented** — the guide explicitly treats it as a future option rather than a v1 requirement. The envelope protocol and bus/presence abstractions are transport-agnostic, so a WebTransport backend could be added later without changing the message model.

### Reliability (per the build guide)

- **Heartbeats** — native WebSocket ping/pong; connections that miss too many pongs are terminated.
- **Rate limiting** — token bucket (capacity 20, refill 10/s) per connection; `resume` is exempt.
- **Backpressure** — `ws.bufferedAmount` is watched; non-critical frames are dropped past a soft limit and the socket is terminated past a hard limit.
- **Reconnection** — client uses exponential backoff (1s → 30s) with +20% jitter, then sends `resume` with per-room `lastSeen` cursors so it replays only what it missed.

### Security notes

- Message text is rendered with `textContent` (no `innerHTML`), so user input is XSS-safe.
- Names and messages are length-capped server-side (30 / 2000 chars).
- The WebSocket upgrade rejects unknown origins and missing/invalid session tokens.
- Posting, reading history and typing require membership of the room. DMs can only be joined or resumed by their two parties (checked by display name, so two users sharing a name can still collide; see limitations).
- Messages persist to SQLite across restarts; in-memory room membership is re-derived on `resume`.

## Roadmap

See [ROADMAP.md](./ROADMAP.md) for planned future additions (accounts, UX polish, rich messaging, E2EE, deployment).

## Scope & limitations

- No accounts yet — identity is a display name + session token; two users with the same name can still collide in DMs.
- Room membership is in-memory and re-derived on reconnect; message history is durable.
- Single-node by default — the Redis fan-out/presence layers (guide steps 13–15) are wired up and checked offline, but not yet run against a real Redis server, and off unless `FANOUT_MODE` is set, keeping local dev dependency-free.
