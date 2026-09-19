'use strict';

// STEP 14 (build guide): presence in a shared store.
// A local Map of "who's online" only works within one process. For
// horizontal scaling, presence lives in a shared store: one key per online
// user with a short TTL (e.g. 30s), renewed on every heartbeat. A user is
// "online" as long as their key hasn't expired. Simple, eventually
// consistent, battle-tested.
//
// MemoryPresence is the zero-dependency default (single process).
// RedisPresence uses one key per user with a TTL, renewed on heartbeat.

class Presence {
  /** Establish the underlying connection (no-op for in-memory). */
  async connect() {}
  /** Mark a user online (create or renew the TTL). */
  touch(name) {}
  /** Remove a user (they disconnected). */
  remove(name) {}
  /** Names currently online (TTL not expired). Always synchronous. */
  online() { return []; }
  /** Release resources. */
  async close() {}
}

// ---------- in-process (default, zero external deps) ----------
class MemoryPresence extends Presence {
  constructor(ttlMs = 30000) {
    super();
    this.ttlMs = ttlMs;
    this.users = new Map(); // name -> lastSeenMs
  }
  touch(name) { this.users.set(name, Date.now()); }
  remove(name) { this.users.delete(name); }
  online() {
    const now = Date.now();
    const out = [];
    for (const [name, ts] of this.users) {
      if (now - ts <= this.ttlMs) out.push(name);
      else this.users.delete(name);
    }
    return out.sort();
  }
  async close() { this.users.clear(); }
}

// ---------- Redis (one key per user, short TTL, renewed on heartbeat) ----------
// `online()` must stay synchronous for the server, so this variant keeps a
// cached snapshot that a background loop refreshes from Redis. touch/remove
// also update the cache immediately for responsiveness.
class RedisPresence extends Presence {
  constructor(redisUrl, { ttlSec = 30, refreshMs = 5000 } = {}) {
    super();
    this.redisUrl = redisUrl;
    this.ttlSec = ttlSec;
    this.refreshMs = refreshMs;
    this.client = null;
    this.prefix = 'whisper:presence:';
    this.cache = new Set();
    this._timer = null;
  }
  async connect() {
    const { createClient } = require('redis');
    this.client = createClient({ url: this.redisUrl });
    await this.client.connect();
    await this._refresh();
    this._timer = setInterval(() => this._refresh().catch(() => {}), this.refreshMs);
    this._timer.unref();
  }
  async _refresh() {
    if (!this.client) return;
    // Use SCAN (incremental, O(1) per call) instead of KEYS (O(N), blocks
    // Redis while scanning the whole keyspace).
    const next = new Set();
    let cursor = '0';
    do {
      const [c, keys] = await this.client.scan(cursor, { MATCH: this.prefix + '*', COUNT: 100 });
      cursor = c;
      for (const k of keys) next.add(k.slice(this.prefix.length));
    } while (cursor !== '0');
    this.cache = next;
  }
  touch(name) {
    if (this.client) this.client.set(this.prefix + name, '1', { EX: this.ttlSec });
    this.cache.add(name);
  }
  remove(name) {
    if (this.client) this.client.del(this.prefix + name);
    this.cache.delete(name);
  }
  online() {
    return [...this.cache].sort();
  }
  async close() {
    if (this._timer) clearInterval(this._timer);
    if (this.client) await this.client.quit();
  }
}

// ---------- factory ----------
function createPresence(config) {
  const mode = String(config.mode || 'memory').toLowerCase();
  // Both 'redis' (pub/sub fanout) and 'streams' (durable fanout) run
  // multi-process, so presence must be shared. RedisPresence is key/TTL-based
  // and is correct for both — only 'memory' stays in-process.
  if (mode === 'redis' || mode === 'streams') return new RedisPresence(config.redisUrl, config);
  return new MemoryPresence(config.ttlMs);
}

module.exports = { Presence, MemoryPresence, RedisPresence, createPresence };
