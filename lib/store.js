'use strict';

// STEP 9 (build guide): message store.
// SQLite via Node's built-in node:sqlite (zero native deps, single file).
// Index on (room_id, ts) for fast "recent messages" queries.

const { DatabaseSync } = require('node:sqlite');
const fs = require('fs');
const path = require('path');

class Store {
  constructor(dbPath) {
    // Ensure the parent directory exists (e.g. data/ isn't tracked by git,
    // so a fresh clone won't have it).
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode = WAL;');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS messages (
        id      TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        sender  TEXT NOT NULL,
        text    TEXT NOT NULL,
        ts      INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_messages_room_ts ON messages (room_id, ts);
      CREATE TABLE IF NOT EXISTS sessions (
        id         TEXT PRIMARY KEY,
        name       TEXT NOT NULL,
        last_seen  INTEGER NOT NULL
      );
    `);

    this.insertMsg = this.db.prepare(
      'INSERT INTO messages (id, room_id, sender, text, ts) VALUES (?, ?, ?, ?, ?)'
    );
    this.msgAfter = this.db.prepare(
      'SELECT id, room_id, sender, text, ts FROM messages WHERE room_id = ? AND id > ? ORDER BY id ASC LIMIT ?'
    );
    this.recent = this.db.prepare(
      'SELECT id, room_id, sender, text, ts FROM messages WHERE room_id = ? ORDER BY ts DESC LIMIT ?'
    );
    this.before = this.db.prepare(
      'SELECT id, room_id, sender, text, ts FROM messages WHERE room_id = ? AND id < ? ORDER BY ts DESC LIMIT ?'
    );
    this.upsertSession = this.db.prepare(`
      INSERT INTO sessions (id, name, last_seen) VALUES (?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET name = excluded.name, last_seen = excluded.last_seen
    `);
    // NOTE: named `sessionStmt` (not `getSession`) so it doesn't shadow the
    // `getSession(sessionId)` prototype method below.
    this.sessionStmt = this.db.prepare('SELECT id, name, last_seen FROM sessions WHERE id = ?');
  }

  saveMessage(msg) {
    this.insertMsg.run(msg.id, msg.roomId, msg.sender, msg.text, msg.ts);
  }

  /** Messages in a room strictly after a given ULID (for resume/replay). */
  messagesAfter(roomId, afterId, limit) {
    return this.msgAfter.all(roomId, afterId, limit).map(rowToMsg);
  }

  /** Last N messages in a room, ascending (for history-on-join). */
  recentMessages(roomId, limit) {
    return this.recent.all(roomId, limit).reverse().map(rowToMsg);
  }

  /** Messages before a cursor, ascending (for "load older" pagination). */
  messagesBefore(roomId, beforeId, limit) {
    return this.before.all(roomId, beforeId, limit).reverse().map(rowToMsg);
  }

  saveSession(sessionId, name) {
    this.upsertSession.run(sessionId, name, Date.now());
  }

  getSession(sessionId) {
    return this.sessionStmt.get(sessionId) || null;
  }

  close() {
    this.db.close();
  }
}

function rowToMsg(row) {
  return {
    id: row.id,
    roomId: row.room_id,
    sender: row.sender,
    text: row.text,
    ts: row.ts,
  };
}

module.exports = { Store, defaultDbPath: path.join(__dirname, '..', 'data', 'whisper.db') };
