# Whisper v2 — Roadmap

Ideas for future additions, roughly ordered by priority. Each item notes what it touches (server, client, or both) so it's easy to pick up.

> **v2 status:** the raw-WebSocket rebuild already ships several items that were "future" in v1 — persistence, pagination, rate limiting, heartbeats, backpressure, resumable reconnection, auth-at-upgrade, origin checking, and a pluggable horizontal-scaling layer (Redis fan-out + presence, off by default). Those are marked `[x]` below.

## 1. Persistence

**Why:** State is in-memory — a server restart wipes all rooms, users, and messages.

- [x] **Message history** — stored per room in SQLite (`node:sqlite`, WAL). Recent history loads on `join-room` / `dm-open`.
- [ ] **Room persistence** — group rooms survive restarts; DMs re-derive from known user pairs. *(v2: membership is in-memory and re-derived on `resume`; messages are durable.)*
- [ ] **User identity** — stable user IDs (see Accounts) so history can be attributed across sessions.
- [x] **Pagination** — `history` event with `before` cursor + `limit` instead of dumping everything.

## 2. Accounts & identity

**Why:** DMs are keyed by display name, so two users with the same name collide.

- [ ] **Stable user IDs** — server-issued UUID per user (cookie or token), display name becomes a mutable profile field.
- [ ] **Optional login** — simple username + password (bcrypt + session token) or OAuth (GitHub) for people who want persistent identity.
- [ ] **Profiles** — avatar (emoji or image URL), status text, "away" state.
- [ ] **Block/mute** — per-user block list; muted users' messages hidden client-side.

## 3. UX polish

- [ ] **Unread badges** — per-room unread counts in the sidebar; mark read on focus.
- [ ] **Message timestamps** — show time on hover / day dividers between messages.
- [ ] **Edit & delete** — `message-edit` / `message-delete` events (own messages only); show "edited" marker.
- [ ] **Reactions** — emoji reactions on messages, toggle on hover.
- [ ] **Search** — full-text search across rooms (server-side once history exists).
- [ ] **Notifications** — browser Notification API for DMs when the tab is unfocused; optional sound.
- [ ] **Mobile layout** — responsive single-pane view with room list as a drawer.
- [ ] **Theme toggle** — light/dark, persisted in `localStorage`.
- [ ] **Keyboard shortcuts** — `Ctrl+K` room switcher, `Esc` to close modal, `/` to focus input.

## 4. Richer messaging

- [ ] **Markdown / code blocks** — render a safe subset (bold, italic, inline code, fenced blocks) with a small sanitizer.
- [ ] **File sharing** — upload to server (size-capped), download links in chat; images inline.
- [ ] **Voice notes** — record in browser (MediaRecorder), send as audio blob.
- [ ] **Typing indicator in DMs** — already works in rooms; verify/extend for DM panes.
- [ ] **Message threading** — reply-to with a thread view (bigger lift; needs history first).

## 5. Rooms & channels

- [ ] **Room topics/descriptions** — editable by creator, shown in header.
- [ ] **Roles** — creator/moderator can kick members, rename room, pin messages.
- [ ] **Pinned messages** — pin/unpin, shown in room header.
- [ ] **Invite links** — shareable URL that auto-joins a room.
- [ ] **Public room directory** — browse and join rooms created by others.

## 6. Reliability & scale

- [x] **Rate limiting** — token-bucket throttle per connection (capacity 20, refill 10/s) to curb spam.
- [x] **Heartbeat / stale socket cleanup** — native WebSocket ping/pong; connections missing too many pongs are terminated.
- [x] **Backpressure** — `ws.bufferedAmount` watched; non-critical frames dropped past a soft limit, socket terminated past a hard limit.
- [x] **Resumable reconnection** — client exponential backoff + jitter, then `resume` with per-room `lastSeen` cursors replays missed messages.
- [ ] **Structured logging** — replace console noise with leveled logs (pino/winston).
- [ ] **Health endpoint** — `GET /healthz` for uptime monitoring.
- [x] **Horizontal scaling** — fan-out + presence abstracted behind `lib/bus.js` / `lib/presence.js` (guide steps 13–15). Zero-dep in-memory default; `FANOUT_MODE=redis|streams` + `REDIS_URL` switches to Redis Pub/Sub or Streams for multi-instance. *Wired and tested, off by default.*
- [ ] **WebTransport transport** — HTTP/3/QUIC backend (guide step 16). *Documented future upgrade path, not a v1 requirement; protocol + bus/presence are already transport-agnostic.*
- [x] **Config via env** — `PORT`, `DB_PATH`, `FANOUT_MODE`, `REDIS_URL`, and rate-limit/backpressure constants are env/configurable.

## 7. Encryption & security

**Why:** A messaging app carries private conversations. Right now everything is plaintext over HTTP and the server holds all messages in memory.

- [ ] **Transport encryption (TLS/HTTPS)** — serve over HTTPS (reverse proxy or PaaS-provided TLS). Encrypts data in transit and unlocks browser notifications + MediaRecorder. *Low effort, high value — do this before going public.*
- [ ] **End-to-end encryption (E2EE)** — encrypt messages client-side so the server only ever sees ciphertext. Requires key exchange (per-room / per-DM shared secret) and key management. *High value as a privacy differentiator, but high effort — and it means the server can no longer search or store readable history.*
- [ ] **Key management** — how keys are generated, stored, and rotated (per-user and per-room). Pairs with Accounts (stable IDs).
- [ ] **Encryption at rest** — if we add persistence, encrypt the stored messages / DB.
- [ ] **Message integrity** — sign/HMAC messages so they can't be tampered with in transit.

## 8. Testing & quality

- [ ] **Unit tests** — extract server logic (room model, DM keying) into testable modules; cover edge cases (same-name users, rapid join/leave).
- [ ] **CI** — GitHub Actions: `npm ci && npm test` on push (server must be started in the workflow).
- [ ] **Lint** — ESLint for server + client JS.
- [ ] **Load test** — simulate 50–100 concurrent sockets to find bottlenecks.

## 9. Deployment

- [ ] **Dockerfile** — containerize for one-command deploys.
- [ ] **PaaS deploy** — Render/Railway/Fly.io with a public URL (needs WebSocket support).
- [ ] **HTTPS** — required for browser notifications and MediaRecorder on non-localhost.
- [ ] **Process manager** — PM2 or systemd unit for bare-metal hosting.

## Suggested order

1. ~~**Persistence** (history + rooms)~~ — **done in v2** (SQLite history + pagination); room persistence still open
2. **Accounts** (stable IDs) — fixes the same-name DM collision
3. **UX polish** (unread badges, timestamps, edit/delete) — makes it feel like a real chat app
4. **Richer messaging** (markdown, file sharing)
5. **TLS/HTTPS** — required before going public (and unlocks notifications/voice)
6. **Testing/CI + deployment** — so it can live somewhere public
7. **E2EE** — the big privacy differentiator; do last, since it constrains search/history
