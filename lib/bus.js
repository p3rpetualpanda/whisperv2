'use strict';

// STEP 13 (build guide): pub/sub fanout layer.
// Abstraction over the message bus so the server can run as a single
// process (MemoryBus, zero external deps) or scale horizontally
// (RedisBus / RedisStreamBus, one channel per room).
//
// STEP 15 (build guide): durable fanout via Redis Streams (RedisStreamBus),
// which persists messages briefly and supports consumer groups / replay so
// an instance can catch up after reconnecting to the bus itself.
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
    this.sub.on('message', (channel, message) => {
      let env;
      try { env = JSON.parse(message); } catch { return; }
      const set = this.handlers.get(channel);
      if (!set) return;
      for (const h of set) {
        try { h(env); } catch (e) { console.error('bus handler error', e); }
      }
    });
  }
  publish(roomId, env) {
    if (this.client) this.client.publish(roomId, JSON.stringify(env));
  }
  subscribe(roomId, handler) {
    const p = (async () => {
      if (!this.handlers.has(roomId)) this.handlers.set(roomId, new Set());
      if (!this.subscribed.has(roomId)) {
        this.subscribed.add(roomId);
        await this.sub.subscribe(roomId);
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

// ---------- Redis Streams (STEP 15, durable fanout) ----------
class RedisStreamBus extends Bus {
  constructor(redisUrl, { group = 'whisper' } = {}) {
    super();
    this._pending = new Map(); // roomId -> in-flight subscribe promise
    this.redisUrl = redisUrl;
    this.group = group;
    this.consumer = 'inst-' + Math.random().toString(36).slice(2, 10);
    this.client = null;
    this.handlers = new Map(); // roomId -> Set<handler>
    this.streams = new Set();
    this._stopped = false;
  }
  async connect() {
    const { createClient } = require('redis');
    this.client = createClient({ url: this.redisUrl });
    await this.client.connect();
    this._loop();
  }
  async _loop() {
    while (this.client && this.client.isOpen && !this._stopped) {
      try {
        if (this.streams.size === 0) { await new Promise((r) => setTimeout(r, 250)); continue; }
        const results = await this.client.xreadGroup(
          { group: this.group, consumer: this.consumer },
          { streams: [...this.streams], count: 100, block: 5000 }
        );
        if (!results) continue;
        for (const [stream, entries] of results) {
          const set = this.handlers.get(stream);
          if (!set) continue;
          for (const [, fields] of entries) {
            let env;
            try { env = JSON.parse(fields.data); } catch { continue; }
            for (const h of set) {
              try { h(env); } catch (e) { console.error('bus handler error', e); }
            }
          }
        }
      } catch (e) {
        if (this.client && this.client.isOpen) console.error('stream read error', e);
      }
    }
  }
  publish(roomId, env) {
    if (this.client) {
      this.client.xadd(roomId, { data: JSON.stringify(env) }, { NOMKSTREAM: true });
    }
  }
  subscribe(roomId, handler) {
    const p = (async () => {
      if (!this.handlers.has(roomId)) {
        this.handlers.set(roomId, new Set());
        this.streams.add(roomId);
        try {
          await this.client.xgroupCreate(roomId, this.group, '$', { MKSTREAM: true });
        } catch (e) {
          if (!String(e.message).includes('BUSYGROUP')) throw e;
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
        this.streams.delete(roomId);
      }
    }
  }
  async close() {
    this._stopped = true;
    if (this.client) await this.client.quit();
  }
}

// ---------- factory ----------
function createBus(config) {
  const mode = String(config.mode || 'memory').toLowerCase();
  if (mode === 'redis') return new RedisBus(config.redisUrl);
  if (mode === 'streams') return new RedisStreamBus(config.redisUrl, config);
  return new MemoryBus();
}

module.exports = { Bus, MemoryBus, RedisBus, RedisStreamBus, createBus };
