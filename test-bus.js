'use strict';

// Offline check of RedisStreamBus against a fake Redis that returns node-redis v4+ reply
// shapes (xRead -> [{ name, messages: [{ id, message }] }], xRevRange -> [{ id, message }]).
// Proves the broadcast logic: two instances both receive every message published after they
// subscribe. It does not prove compatibility with a real Redis server.

const assert = require('assert');
const { RedisStreamBus } = require('./lib/bus');

function fakeRedis() {
  const streams = new Map(); // key -> [{ seq, id, message }]
  let seq = 0;
  const calls = [];
  function client() {
    return {
      isOpen: false,
      async connect() { this.isOpen = true; },
      async quit() { this.isOpen = false; },
      duplicate() { return client(); },
      async xAdd(key, id, message, opts) {
        calls.push({ cmd: 'xAdd', key, id, opts });
        if (!streams.has(key)) streams.set(key, []);
        const entry = { seq: ++seq, id: `${seq}-0`, message };
        streams.get(key).push(entry);
        return entry.id;
      },
      async xRevRange(key, start, end, { COUNT }) {
        return (streams.get(key) || []).slice(-COUNT).reverse().map(({ id, message }) => ({ id, message }));
      },
      async xRead(list, { COUNT, BLOCK }) {
        const read = () => list
          .map(({ key, id }) => ({
            name: key,
            messages: (streams.get(key) || [])
              .filter((e) => e.seq > Number(id.split('-')[0]))
              .slice(0, COUNT)
              .map(({ id: eid, message }) => ({ id: eid, message })),
          }))
          .filter((s) => s.messages.length);
        const deadline = Date.now() + BLOCK;
        while (Date.now() < deadline) {
          const out = read();
          if (out.length) return out;
          await new Promise((r) => setTimeout(r, 10));
        }
        return null;
      },
    };
  }
  return { createClient: () => client(), calls };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const redis = fakeRedis();
  const a = new RedisStreamBus('redis://fake', { createClient: redis.createClient });
  const b = new RedisStreamBus('redis://fake', { createClient: redis.createClient });
  await a.connect();
  await b.connect();

  await a.publish('room-1', { env: { text: 'before-subscribe' }, critical: true });

  const gotA = [];
  const gotB = [];
  await a.subscribe('room-1', (v) => gotA.push(v.env.text));
  await b.subscribe('room-1', (v) => gotB.push(v.env.text));

  await a.publish('room-1', { env: { text: 'one' }, critical: true });
  await b.publish('room-1', { env: { text: 'two' }, critical: true });
  await sleep(300);

  assert.deepStrictEqual(gotA, ['one', 'two'], 'instance A gets every message, in order');
  assert.deepStrictEqual(gotB, ['one', 'two'], 'instance B gets every message, in order');

  const add = redis.calls.find((c) => c.cmd === 'xAdd');
  assert.strictEqual(add.id, '*');
  assert.deepStrictEqual(add.opts.TRIM, { strategy: 'MAXLEN', strategyModifier: '~', threshold: 1000 });

  await a.close();
  await b.close();
  console.log('PASS - RedisStreamBus broadcasts to every instance (fake Redis)');
})().catch((e) => {
  console.error('FAIL -', e.message);
  process.exit(1);
});
