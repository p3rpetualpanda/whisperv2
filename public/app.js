(() => {
  'use strict';

  // DOM refs
  const nameOverlay = document.getElementById('name-overlay');
  const nameInput = document.getElementById('name-input');
  const nameSubmit = document.getElementById('name-submit');
  const app = document.getElementById('app');
  const myNameEl = document.getElementById('my-name');
  const roomListEl = document.getElementById('room-list');
  const onlineListEl = document.getElementById('online-list');
  const newRoomBtn = document.getElementById('new-room-btn');
  const chatTitle = document.getElementById('chat-title');
  const chatSubtitle = document.getElementById('chat-subtitle');
  const leaveBtn = document.getElementById('leave-btn');
  const messagesEl = document.getElementById('messages');
  const typingEl = document.getElementById('typing');
  const composer = document.getElementById('composer');
  const msgInput = document.getElementById('msg-input');
  const roomModal = document.getElementById('room-modal');
  const roomNameInput = document.getElementById('room-name-input');
  const roomCancel = document.getElementById('room-cancel');
  const roomCreate = document.getElementById('room-create');

  // ---------- client state ----------
  let myName = null;
  let token = null;
  let ws = null;
  let currentRoom = { id: 'lobby', name: 'Lobby', kind: 'lobby' };
  const roomMeta = new Map();      // roomId -> { name, kind, peer }
  const myRooms = new Set(['lobby']); // rooms I'm a member of (for resume)
  const lastSeen = new Map();      // roomId -> last message id seen (STEP 8)
  const typingTimers = new Map();  // roomId -> timer
  const typingDebounce = { timer: null };

  // Reconnection (STEP 7)
  let intentionalClose = false;
  let reconnectAttempt = 0;
  let reconnectTimer = null;
  const RECONNECT_BASE_MS = 1000;
  const RECONNECT_MAX_MS = 30000;

  roomMeta.set('lobby', { name: 'Lobby', kind: 'lobby', peer: null });

  // ---------- connection (STEP 1, 4, 7) ----------
  function wsUrl() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    return proto + '//' + location.host + '/ws?token=' + encodeURIComponent(token);
  }

  function connect() {
    intentionalClose = false;
    ws = new WebSocket(wsUrl());

    ws.onopen = () => {
      reconnectAttempt = 0;
      setConnStatus(true);
      // STEP 8: resume — replay anything we missed while disconnected.
      const lastSeenObj = {};
      for (const [roomId, id] of lastSeen) lastSeenObj[roomId] = id;
      send({ type: 'resume', payload: { rooms: [...myRooms], lastSeen: lastSeenObj } });
      send({ type: 'list' });
    };

    ws.onmessage = (ev) => {
      let env;
      try { env = JSON.parse(ev.data); } catch { return; }
      handleEnvelope(env);
    };

    ws.onclose = () => {
      setConnStatus(false);
      if (intentionalClose) return;
      scheduleReconnect();
    };

    ws.onerror = () => { /* onclose follows */ };
  }

  // STEP 7: exponential backoff with jitter, capped.
  function scheduleReconnect() {
    if (reconnectTimer) return;
    const delay = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * Math.pow(2, reconnectAttempt));
    reconnectAttempt += 1;
    const jitter = delay * 0.2 * Math.random();
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, delay + jitter);
  }

  function send(payload) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload));
      return true;
    }
    return false;
  }

  function setConnStatus(online) {
    chatSubtitle.dataset.online = online ? '1' : '0';
    if (!online && myName) {
      // transient banner via subtitle
      const meta = roomMeta.get(currentRoom.id);
      chatSubtitle.textContent = 'reconnecting…';
      if (meta) {
        // restore label once back online
      }
    }
  }

  // ---------- envelope dispatch (STEP 2, 3) ----------
  function handleEnvelope(env) {
    const p = env.payload || {};
    switch (env.type) {
      case 'hello-ok':
        myName = p.name;
        myNameEl.textContent = myName;
        nameOverlay.classList.add('hidden');
        app.classList.remove('hidden');
        if (p.rooms) renderRooms(p.rooms);
        if (p.online) renderOnline(p.online);
        break;

      case 'list':
        if (p.rooms) renderRooms(p.rooms);
        if (p.online) renderOnline(p.online);
        break;

      case 'presence':
        if (p.online) renderOnline(p.online);
        break;

      case 'rooms':
        if (p.rooms) renderRooms(p.rooms);
        break;

      case 'message': {
        const msg = p.msg;
        if (!msg) break;
        // STEP 8: track last-seen id per room for resumable reconnect
        lastSeen.set(msg.roomId, msg.id);
        if (msg.roomId === currentRoom.id) addMessage(msg);
        break;
      }

      case 'resumed':
        // replayed messages already rendered via 'message'
        break;

      case 'system':
        if (env.room === currentRoom.id || currentRoom.id === 'lobby') addSystem(p.text);
        break;

      case 'room-created':
        if (p.room) {
          roomMeta.set(p.room.id, { name: p.room.name, kind: 'group', peer: null });
          myRooms.add(p.room.id);
          selectRoom(p.room.id);
        }
        break;

      case 'joined':
        if (p.room) {
          roomMeta.set(p.room.id, { name: p.room.name, kind: p.room.kind, peer: null });
          myRooms.add(p.room.id);
        }
        break;

      case 'dm-opened':
        roomMeta.set(p.roomId, { name: 'DM with ' + p.peer, kind: 'dm', peer: p.peer });
        myRooms.add(p.roomId);
        selectRoom(p.roomId);
        break;

      case 'typing':
        if (p.roomId !== currentRoom.id) return;
        if (p.typing) {
          showTyping(p.name);
          clearTimeout(typingTimers.get(p.roomId));
          typingTimers.set(p.roomId, setTimeout(clearTyping, 3000));
        } else {
          clearTyping();
        }
        break;

      case 'history':
        renderHistory(p.messages || [], p.hasMore, env.room);
        break;

      case 'rate-limited':
        addSystem('Slow down — you are sending messages too fast.');
        break;

      case 'error':
        addSystem(p.error || 'Error');
        break;
    }
  }

  // ---------- rendering ----------
  function fmtTime(ts) {
    return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }

  function addMessage(msg) {
    const div = document.createElement('div');
    const mine = myName && msg.sender === myName;
    div.className = 'msg ' + (mine ? 'me' : 'them');
    const meta = document.createElement('div');
    meta.className = 'meta';
    const sender = document.createElement('span');
    sender.textContent = msg.sender;
    const time = document.createElement('span');
    time.textContent = fmtTime(msg.ts);
    meta.append(sender, time);
    const body = document.createElement('div');
    body.textContent = msg.text;
    div.append(meta, body);
    messagesEl.appendChild(div);
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  function addSystem(text) {
    const div = document.createElement('div');
    div.className = 'msg system';
    div.textContent = text;
    messagesEl.appendChild(div);
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  // STEP 10: render a page of history (oldest -> newest) at the top.
  function renderHistory(messages, hasMore, roomId) {
    if (roomId && roomId !== currentRoom.id) return;
    // Remove any existing "load older" button
    const existing = messagesEl.querySelector('.load-older');
    if (existing) existing.remove();

    const frag = document.createDocumentFragment();
    for (const m of messages) {
      const div = document.createElement('div');
      const mine = myName && m.sender === myName;
      div.className = 'msg ' + (mine ? 'me' : 'them');
      const meta = document.createElement('div');
      meta.className = 'meta';
      const sender = document.createElement('span');
      sender.textContent = m.sender;
      const time = document.createElement('span');
      time.textContent = fmtTime(m.ts);
      meta.append(sender, time);
      const body = document.createElement('div');
      body.textContent = m.text;
      div.append(meta, body);
      frag.appendChild(div);
    }
    messagesEl.prepend(frag);

    if (hasMore && messages.length > 0) {
      const oldestId = messages[0].id;
      const btn = document.createElement('button');
      btn.className = 'load-older';
      btn.textContent = 'Load older messages';
      btn.addEventListener('click', () => {
        btn.remove();
        send({ type: 'history', payload: { roomId: currentRoom.id, beforeId: oldestId } });
      });
      messagesEl.prepend(btn);
    }
  }

  function renderRooms(rooms) {
    roomListEl.innerHTML = '';
    const lobbyLi = document.createElement('li');
    lobbyLi.textContent = 'Lobby';
    lobbyLi.dataset.roomId = 'lobby';
    if (currentRoom.id === 'lobby') lobbyLi.classList.add('active');
    lobbyLi.addEventListener('click', () => selectRoom('lobby'));
    roomListEl.appendChild(lobbyLi);

    for (const r of rooms) {
      roomMeta.set(r.id, {
        name: r.kind === 'dm' ? (r.peer || 'DM') : r.name,
        kind: r.kind,
        peer: r.kind === 'dm' ? (r.peer || null) : null,
      });
      const li = document.createElement('li');
      const label = document.createElement('span');
      label.textContent = r.kind === 'dm' ? '💬 ' + (r.peer || 'DM') : r.name;
      const count = document.createElement('span');
      count.className = 'count';
      count.textContent = r.members;
      li.append(label, count);
      li.dataset.roomId = r.id;
      if (currentRoom.id === r.id) li.classList.add('active');
      li.addEventListener('click', () => selectRoom(r.id));
      roomListEl.appendChild(li);
    }
  }

  function renderOnline(online) {
    onlineListEl.innerHTML = '';
    for (const name of online) {
      const li = document.createElement('li');
      const dot = document.createElement('span');
      dot.className = 'dot';
      const label = document.createElement('span');
      label.textContent = name + (name === myName ? ' (you)' : '');
      li.append(dot, label);
      if (name === myName) {
        li.classList.add('me');
      } else {
        li.addEventListener('click', () => openDm(name));
      }
      onlineListEl.appendChild(li);
    }
  }

  function selectRoom(roomId) {
    const meta = roomMeta.get(roomId) || { name: roomId, kind: 'group', peer: null };
    currentRoom = { id: roomId, name: meta.name, kind: meta.kind };
    for (const li of roomListEl.children) {
      li.classList.toggle('active', li.dataset.roomId === roomId);
    }
    chatTitle.textContent = meta.kind === 'dm' ? 'DM with ' + (meta.peer || '') : meta.name;
    chatSubtitle.textContent = meta.kind === 'dm' ? 'private chat' : 'group room';
    // Leave is only meaningful for group rooms — not the lobby (always present)
    // or DMs (tied to a peer, not a joinable room).
    leaveBtn.classList.toggle('hidden', meta.kind !== 'group');
    messagesEl.innerHTML = '';
    if (meta.kind === 'dm') addSystem('Start of conversation with ' + (meta.peer || ''));
    if (roomId !== 'lobby') send({ type: 'join-room', payload: { roomId } });
    msgInput.focus();
  }

  function openDm(peerName) {
    send({ type: 'dm-open', payload: { to: peerName } });
  }

  function leaveRoom() {
    if (!currentRoom || currentRoom.kind !== 'group') return;
    const roomId = currentRoom.id;
    send({ type: 'leave-room', payload: { roomId } });
    // Drop the room locally and return to the lobby. The server broadcasts an
    // updated `rooms` list (handleLeaveRoom -> broadcastRooms), which refreshes
    // the sidebar via renderRooms.
    myRooms.delete(roomId);
    roomMeta.delete(roomId);
    selectRoom('lobby');
  }

  function showTyping(name) { typingEl.textContent = name + ' is typing…'; }
  function clearTyping() { typingEl.textContent = ''; }

  // ---------- UI wiring ----------
  function doJoin() {
    const name = nameInput.value.trim();
    if (!name) return;
    nameSubmit.disabled = true;
    // STEP 4: create a session over HTTP, then connect the WebSocket with the token.
    fetch('/api/session', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    })
      .then((r) => r.json())
      .then((res) => {
        if (res && res.ok) {
          token = res.token;
          myName = res.name;
          connect();
        } else {
          nameSubmit.disabled = false;
          nameInput.placeholder = (res && res.error) || 'Name required';
        }
      })
      .catch(() => {
        nameSubmit.disabled = false;
        nameInput.placeholder = 'Network error — try again';
      });
  }

  nameSubmit.addEventListener('click', doJoin);
  nameInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') doJoin(); });

  composer.addEventListener('submit', (e) => {
    e.preventDefault();
    const text = msgInput.value.trim();
    if (!text) return;
    send({ type: 'message', payload: { roomId: currentRoom.id, text } });
    msgInput.value = '';
    send({ type: 'typing', payload: { roomId: currentRoom.id, typing: false } });
  });

  msgInput.addEventListener('input', () => {
    send({ type: 'typing', payload: { roomId: currentRoom.id, typing: true } });
    clearTimeout(typingDebounce.timer);
    typingDebounce.timer = setTimeout(() => {
      send({ type: 'typing', payload: { roomId: currentRoom.id, typing: false } });
    }, 2000);
  });

  // Leave current group room
  leaveBtn.addEventListener('click', leaveRoom);

  // New room modal
  newRoomBtn.addEventListener('click', () => {
    roomNameInput.value = '';
    roomModal.classList.remove('hidden');
    roomNameInput.focus();
  });
  roomCancel.addEventListener('click', () => roomModal.classList.add('hidden'));
  roomCreate.addEventListener('click', () => {
    const name = roomNameInput.value.trim();
    if (!name) return;
    roomModal.classList.add('hidden');
    send({ type: 'create-room', payload: { name } });
  });
  roomNameInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') roomCreate.click(); });

  // Focus name input on load
  nameInput.focus();
})();
