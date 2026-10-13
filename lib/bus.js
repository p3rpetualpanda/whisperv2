'use strict';

// STEP 13 (build guide): pub/sub fanout layer.
// Abstraction over the message bus so the server can run as a single
// process (MemoryBus, zero external deps) or scale horizontally
// (RedisBus / RedisStreamBus, one channel per room).
//
// STEP 15 (build guide): fanout via Redis Streams (RedisStreamBus). Entries are
// kept briefly (capped with MAXLEN) so a reader that falls behind for a moment
// still gets them in order. A restarted instance starts from "now": message
// history for users comes from SQLite plus the client's resume.
//
// The `redis` npm package is only required when a Redis-backed bus is
// selected (FANOUT_MODE=redis|streams). The default MemoryBus needs nothing.

class Bus {
  /** Establish the underlying connection (no-op for in-memory). */
  async connect() {}
  /** Publish a value to a room channel. */
  publish(roomId, value) {}
  /** Subscribe a handler to a room channel. */
  subscribe(roomId, handler) {}
  /** Resolve once the room's subscription is established (no-op for in-memory). */
  ready(roomId) { return Promise.resolve(); }
  /** Remove a handler from a room channel. */
  unsubscribe(roomId, handler) {}
  /** Release resources. */
  async close() {}
}

// ---------- in-process (default, zero external deps) ----------
class MemoryBus extends Bus {
  constructor() {
    super();
    this.handlers = new Map(); // roomId -> Set<handler>
  }
  publish(roomId, env) {
    const set = this.handlers.get(roomId);
    if (!set) return;
    for (const h of set) {
      try { h(env); } catch (e) { console.error('bus handler error', e); }
    }
  }
  subscribe(roomId, handler) {
    if (!this.handlers.has(roomId)) this.handlers.set(roomId, new Set());
    this.handlers.get(roomId).add(handler);
  }
  unsubscribe(roomId, handler) {
    const set = this.handlers.get(roomId);
    if (set) set.delete(handler);
  }
  async close() { this.handlers.clear(); }
}

// ---------- Redis Pub/Sub (STEP 13) ----------
class RedisBus extends Bus {
  constructor(redisUrl) {
    super();
    this.redisUrl = redisUrl;
    this.client = null;
    this.sub = null;
    this.handlers = new Map(); // roomId -> Set<handler>
    this.subscribed = new Set(); // channels subscribed at the Redis level
    this._pending = new Map(); // roomId -> in-flight subscribe promise
  }
  async connect() {
    const { createClient } = require('redis');
    this.client = createClient({ url: this.redisUrl });
    this.sub = this.client.duplicate();
    await Promise.all([this.client.connect(), this.sub.connect()]);
  }
  _dispatch(channel, message) {
    let env;
    try { env = JSON.parse(message); } catch { return; }
    const set = this.handlers.get(channel);
    if (!set) return;
    for (const h of set) {
      try { h(env); } catch (e) { console.error('bus handler error', e); }
    }
  }
  publish(roomId, env) {
    if (this.client) this.client.publish(roomId, JSON.stringify(env));
  }
  subscribe(roomId, handler) {
    const p = (async () => {
      if (!this.handlers.has(roomId)) this.handlers.set(roomId, new Set());
      if (!this.subscribed.has(roomId)) {
        this.subscribed.add(roomId);
        try {
          // node-redis v4+: messages arrive via this listener, not a 'message' event.
          await this.sub.subscribe(roomId, (message, channel) => this._dispatch(channel, message));
        } catch (e) {
          this.subscribed.delete(roomId); // let the next subscribe retry
          throw e;
        }
      }
      this.handlers.get(roomId).add(handler);
    })();
    this._pending.set(roomId, p);
    return p;
  }
  ready(roomId) {
    return this._pending.get(roomId) || Promise.resolve();
  }
  async unsubscribe(roomId, handler) {
    const set = this.handlers.get(roomId);
    if (set) {
      set.delete(handler);
      if (set.size === 0) {
        this.handlers.delete(roomId);
        if (this.subscribed.has(roomId)) {
          this.subscribed.delete(roomId);
          await this.sub.unsubscribe(roomId);
        }
      }
    }
  }
  async close() {
    if (this.sub) await this.sub.quit();
    if (this.client) await this.client.quit();
  }
}

// ---------- Redis Streams (STEP 15, broadcast fanout) ----------
// Plain XREAD (no consumer groups): every instance reads every entry, which is what room
// broadcast needs. A consumer group would split entries between instances, and per-instance
// groups would pile up pending entries and dead groups in Redis. Streams are capped with
// MAXLEN ~ so they can't grow without bound.
const STREAM_MAXLEN = 1000;
const STREAM_BLOCK_MS = 1000; // how long a new room can wait before the reader picks it up

class RedisStreamBus extends Bus {
  constructor(redisUrl, { createClient } = {}) {
    super();
    this._pending = new Map(); // roomId -> in-flight subscribe promise
    this.redisUrl = redisUrl;
    this._createClient = createClient; // injectable for tests; defaults to the redis package
    this.client = null; // publishes
    this.reader = null; // blocking XREAD loop; separate connection so publishes never queue behind it
    this.handlers = new Map(); // roomId -> Set<handler>
    this.lastIds = new Map(); // roomId -> last stream entry id seen
    this._stopped = false;
  }
  async connect() {
    const createClient = this._createClient || require('redis').createClient;
    this.client = createClient({ url: this.redisUrl });
    this.reader = this.client.duplicate();
    await Promise.all([this.client.connect(), this.reader.connect()]);
    this._loop();
  }
  async _loop() {
    while (!this._stopped && this.reader && this.reader.isOpen) {
      try {
        if (this.lastIds.size === 0) { await new Promise((r) => setTimeout(r, 250)); continue; }
        const streams = [...this.lastIds].map(([key, id]) => ({ key, id }));
        const results = await this.reader.xRead(streams, { COUNT: 100, BLOCK: STREAM_BLOCK_MS });
        for (const { name, messages } of results || []) {
          for (const { id, message } of messages) {
            if (this.lastIds.has(name)) this.lastIds.set(name, id);
            let env;
            try { env = JSON.parse(message.data); } catch { continue; }
            for (const h of this.handlers.get(name) || []) {
              try { h(env); } catch (e) { console.error('bus handler error', e); }
            }
          }
        }
      } catch (e) {
        if (this._stopped) return;
        console.error('stream read error', e);
        await new Promise((r) => setTimeout(r, 1000)); // don't spin on a persistent error
      }
    }
  }
  publish(roomId, env) {
    if (!this.client) return;
    return this.client.xAdd(roomId, '*', { data: JSON.stringify(env) }, {
      TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: STREAM_MAXLEN },
    });
  }
  subscribe(roomId, handler) {
    const p = (async () => {
      if (!this.handlers.has(roomId)) {
        this.handlers.set(roomId, new Set());
        // Start after the newest existing entry, so only messages published from now on are
        // delivered (history comes from SQLite). '0-0' when the stream doesn't exist yet.
        const [last] = await this.client.xRevRange(roomId, '+', '-', { COUNT: 1 });
        this.lastIds.set(roomId, last ? last.id : '0-0');
      }
      this.handlers.get(roomId).add(handler);
    })();
    this._pending.set(roomId, p);
    return p;
  }
  ready(roomId) {
    return this._pending.get(roomId) || Promise.resolve();
  }
  async unsubscribe(roomId, handler) {
    const set = this.handlers.get(roomId);
    if (set) {
      set.delete(handler);
      if (set.size === 0) {
        this.handlers.delete(roomId);
        this.lastIds.delete(roomId);
      }
    }
  }
  async close() {
    this._stopped = true;
    if (this.reader) await this.reader.quit(); // returns once the current BLOCK (<= 1 s) ends
    if (this.client) await this.client.quit();
  }
}

// ---------- factory ----------
function createBus(config) {
  const mode = String(config.mode || 'memory').toLowerCase();
  if (mode === 'redis') return new RedisBus(config.redisUrl);
  if (mode === 'streams') return new RedisStreamBus(config.redisUrl);
  return new MemoryBus();
}

module.exports = { Bus, MemoryBus, RedisBus, RedisStreamBus, createBus };
