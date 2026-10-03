/* ============================================================
   NecShare — private peer-to-peer chat, files and calls.
   Signaling: Firebase Realtime Database. Media/data: WebRTC P2P.
   No accounts. Pairing via one-time 6-character codes.
   ============================================================ */
(function () {
  "use strict";

  /* ---------------- utils ---------------- */
  function $(id) { return document.getElementById(id); }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function uid() {
    var s = "";
    for (var i = 0; i < 16; i++) s += Math.floor(Math.random() * 16).toString(16);
    return Date.now().toString(36) + s;
  }

  function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

  function fmtSize(b) {
    if (b < 1024) return b + " B";
    if (b < 1024 * 1024) return (b / 1024).toFixed(1) + " KB";
    return (b / (1024 * 1024)).toFixed(1) + " MB";
  }

  function fmtTime(ts) {
    var d = new Date(ts || Date.now());
    var h = d.getHours(), m = d.getMinutes();
    return (h < 10 ? "0" + h : h) + ":" + (m < 10 ? "0" + m : m);
  }

  function fmtCountdown(ms) {
    var s = Math.max(0, Math.ceil(ms / 1000));
    var m = Math.floor(s / 60);
    s = s % 60;
    return (m < 10 ? "0" + m : m) + ":" + (s < 10 ? "0" + s : s);
  }

  function toast(msg) {
    var t = $("toast");
    t.textContent = msg;
    t.classList.add("show");
    clearTimeout(t._h);
    t._h = setTimeout(function () { t.classList.remove("show"); }, 3000);
  }

  function showError(elId, msg) {
    var el = $(elId);
    el.textContent = msg || "";
    el.hidden = !msg;
  }

  function base64Encode(ab) {
    var u = new Uint8Array(ab), s = "";
    for (var i = 0; i < u.length; i++) s += String.fromCharCode(u[i]);
    return btoa(s);
  }

  function base64Decode(b64) {
    var s = atob(b64), u = new Uint8Array(s.length);
    for (var i = 0; i < s.length; i++) u[i] = s.charCodeAt(i);
    return u;
  }

  /* ---------------- state ---------------- */
  var CODE_TTL = 5 * 60 * 1000;
  var CODE_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
  var CHUNK = 16 * 1024;
  var MAX_FILE = 100 * 1024 * 1024;

  var state = {
    myId: null,
    code: null,
    codeExpiresAt: 0,
    remoteId: null,
    isOfferer: false,
    pc: null,
    dc: null,
    pendingIce: [],
    pendingReq: null,      // outgoing request {to, reqId}
    requests: {},          // incoming reqId -> {from, at}
    incomingFiles: {},     // fileId -> {name,size,type,total,chunks,received}
    fileUI: {},            // fileId -> {fill, action}
    localStream: null,
    call: null,            // {media:'audio'|'video', dir:'in'|'out', phase}
    callTimer: null,
    callStart: 0,
    presenceRef: null,
    micOn: true,
    camOn: true,
    speakerOn: true
  };

  var db = null;

  function SV() { return firebase.database.ServerValue.TIMESTAMP; }

  /* ---------------- identity ---------------- */
  function getPeerId() {
    var id = null;
    try { id = localStorage.getItem("necshare_peer"); } catch (e) {}
    if (!id) {
      var b = new Uint8Array(16);
      crypto.getRandomValues(b);
      id = "";
      for (var i = 0; i < 16; i++) id += ("0" + b[i].toString(16)).slice(-2);
      try { localStorage.setItem("necshare_peer", id); } catch (e) {}
    }
    return id;
  }

  /* ---------------- pairing codes ---------------- */
  function genCode() {
    var buf = new Uint8Array(6), s = "";
    crypto.getRandomValues(buf);
    for (var i = 0; i < 6; i++) s += CODE_CHARS[buf[i] % CODE_CHARS.length];
    return s;
  }

  async function publishCode() {
    if (state.code) {
      try { await db.ref("codes/" + state.code).remove(); } catch (e) {}
      state.code = null;
    }
    for (var a = 0; a < 5; a++) {
      var code = genCode();
      try {
        var snap = await db.ref("codes/" + code).get();
        if (snap.exists()) {
          var v = snap.val() || {};
          if (v.owner && v.owner !== state.myId && (Date.now() - (v.at || 0)) < CODE_TTL) continue;
        }
        await db.ref("codes/" + code).set({ owner: state.myId, at: SV() });
        state.code = code;
        state.codeExpiresAt = Date.now() + CODE_TTL;
        renderCode();
        return;
      } catch (e) { /* retry */ }
    }
    $("myCode").textContent = "------";
    toast("Could not create a code. Check your connection.");
  }

  function renderCode() {
    $("myCode").textContent = state.code || "------";
    tickCodeTimer();
  }

  function tickCodeTimer() {
    if (!state.code) { $("codeTimer").textContent = "refreshes in --:--"; return; }
    var left = state.codeExpiresAt - Date.now();
    if (left <= 0) { publishCode(); return; }
    $("codeTimer").textContent = "refreshes in " + fmtCountdown(left);
  }

  function inviteLink() {
    var base;
    if (location.protocol === "file:") {
      base = location.href.split("?")[0].split("#")[0];
    } else {
      base = location.origin + location.pathname;
    }
    return base + "?j=" + state.code;
  }

  async function copyText(t, okMsg) {
    try {
      await navigator.clipboard.writeText(t);
      toast(okMsg);
    } catch (e) {
      var ta = document.createElement("textarea");
      ta.value = t;
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand("copy"); toast(okMsg); }
      catch (e2) { toast("Copy failed — long-press to copy manually."); }
      document.body.removeChild(ta);
    }
  }

  /* ---------------- connect (outgoing request) ---------------- */
  async function connect() {
    var raw = $("codeInput").value.trim().toUpperCase();
    var code = raw.replace(/[^A-Z2-9]/g, "").slice(0, 6);
    $("codeInput").value = code;
    if (code.length !== 6) { showError("connectError", "Enter the 6-character code."); return; }
    if (code === state.code) { showError("connectError", "That is your own code. Ask the other person for theirs."); return; }
    if (state.remoteId) { showError("connectError", "Already connected. Disconnect first to pair anew."); return; }
    showError("connectError", "");
    var btn = $("btnConnect");
    btn.disabled = true;
    try {
      var snap = await db.ref("codes/" + code).get();
      if (!snap.exists()) throw new Error("Code not found. It may have expired — ask for a fresh code.");
      var owner = (snap.val() || {}).owner;
      if (!owner || owner === state.myId) throw new Error("This code is not valid.");
      var reqRef = await db.ref("peers/" + owner + "/inbox").push({ from: state.myId, at: SV() });
      state.pendingReq = { to: owner, reqId: reqRef.key };
      $("waitingCard").hidden = false;
      $("waitingSub").textContent = "Request sent to code " + code + ". Waiting for approval...";
    } catch (e) {
      showError("connectError", (e && e.message) || "Could not send the request.");
    }
    btn.disabled = false;
  }

  async function cancelRequest() {
    if (state.pendingReq) {
      try { await db.ref("peers/" + state.pendingReq.to + "/inbox/" + state.pendingReq.reqId).remove(); } catch (e) {}
      state.pendingReq = null;
    }
    $("waitingCard").hidden = true;
    toast("Request cancelled");
  }

  /* ---------------- inbox (incoming requests) ---------------- */
  function watchInbox() {
    var ref = db.ref("peers/" + state.myId + "/inbox");
    ref.on("child_added", function (snap) {
      var v = snap.val() || {};
      if (!v.from) return;
      addRequest(snap.key, v.from, v.at);
    });
    ref.on("child_removed", function (snap) { removeRequest(snap.key); });
  }

  function shortId(id) { return (id || "").slice(0, 8); }

  function addRequest(reqId, fromId, at) {
    if (state.requests[reqId]) return;
    state.requests[reqId] = { from: fromId, at: at || Date.now() };
    renderRequests();
  }

  function removeRequest(reqId) {
    if (!state.requests[reqId]) return;
    delete state.requests[reqId];
    renderRequests();
  }

  function requestCount() { return Object.keys(state.requests).length; }

  function renderRequests() {
    var list = $("reqList");
    var n = requestCount();
    $("reqCount").textContent = n;
    $("reqHintCount").textContent = n;
    var badge = $("tabBadge");
    badge.hidden = n === 0;
    badge.textContent = n;
    $("reqEmpty").hidden = n !== 0;
    $("reqHint").hidden = n === 0;
    list.innerHTML = "";
    Object.keys(state.requests).forEach(function (reqId) {
      var r = state.requests[reqId];
      var card = document.createElement("div");
      card.className = "req-card";
      var info = document.createElement("div");
      info.className = "req-info";
      var title = document.createElement("p");
      title.className = "req-title";
      title.textContent = "Peer " + shortId(r.from);
      var sub = document.createElement("p");
      sub.className = "req-sub";
      sub.textContent = "Wants to connect · " + fmtTime(r.at);
      info.appendChild(title);
      info.appendChild(sub);
      var acts = document.createElement("div");
      acts.className = "req-actions";
      var ok = document.createElement("button");
      ok.className = "btn btn-accept";
      ok.textContent = "Accept";
      ok.addEventListener("click", function () { acceptRequest(reqId, r.from); });
      var no = document.createElement("button");
      no.className = "btn btn-decline";
      no.textContent = "Decline";
      no.addEventListener("click", function () { declineRequest(reqId, r.from); });
      acts.appendChild(ok);
      acts.appendChild(no);
      card.appendChild(info);
      card.appendChild(acts);
      list.appendChild(card);
    });
  }

  async function acceptRequest(reqId, fromId) {
    try { await db.ref("peers/" + state.myId + "/inbox/" + reqId).remove(); } catch (e) {}
    removeRequest(reqId);
    toast("Connecting...");
    await startPairingAsOfferer(fromId);
  }

  async function declineRequest(reqId, fromId) {
    try { await db.ref("peers/" + state.myId + "/inbox/" + reqId).remove(); } catch (e) {}
    removeRequest(reqId);
    try { await signal(fromId, "declined", {}); } catch (e) {}
    toast("Request declined");
  }

  /* ---------------- signals ---------------- */
  function signal(toPeerId, kind, data) {
    return db.ref("signals/" + toPeerId).push({
      from: state.myId, kind: kind, data: data || {}, at: SV()
    });
  }

  function watchSignals() {
    db.ref("signals/" + state.myId).on("child_added", async function (snap) {
      var m = snap.val() || {};
      try { await snap.ref.remove(); } catch (e) {}
      try { await handleSignal(m); }
      catch (e) { toast("Connection error: " + (e && e.message ? e.message : "unknown")); }
    });
  }

  async function handleSignal(m) {
    if (!m || !m.kind) return;
    if (m.kind === "declined") {
      if (state.pendingReq) {
        state.pendingReq = null;
        $("waitingCard").hidden = true;
        toast("Connection request declined");
      }
      return;
    }
    if (m.kind === "offer") { await onOffer(m); return; }
    if (m.kind === "answer") { await onAnswer(m); return; }
    if (m.kind === "ice") { await onIce(m); return; }
  }

  /* ---------------- WebRTC ---------------- */
  var RTC_CFG = { iceServers: [{ urls: "stun:stun.l.google.com:19302" }] };

  function closePC() {
    if (state.dc) { try { state.dc.close(); } catch (e) {} state.dc = null; }
    if (state.pc) { try { state.pc.close(); } catch (e) {} state.pc = null; }
    state.pendingIce = [];
  }

  function createPC() {
    closePC();
    var pc = new RTCPeerConnection(RTC_CFG);
    state.pc = pc;
    pc.onicecandidate = function (e) {
      if (e.candidate && state.remoteId) {
        var c = e.candidate.toJSON ? e.candidate.toJSON() : e.candidate;
        signal(state.remoteId, "ice", c).catch(function () {});
      }
    };
    pc.onconnectionstatechange = function () {
      var s = pc.connectionState;
      if (s === "connected") onConnected();
      else if (s === "failed" || s === "disconnected" || s === "closed") onDisconnected();
    };
    pc.ontrack = function (e) {
      var v = $("remoteVideo");
      if (e.streams && e.streams[0] && v.srcObject !== e.streams[0]) v.srcObject = e.streams[0];
    };
    pc.ondatachannel = function (e) { setupDC(e.channel); };
    return pc;
  }

  async function startPairingAsOfferer(remoteId) {
    state.remoteId = remoteId;
    state.isOfferer = true;
    var pc = createPC();
    var dc = pc.createDataChannel("necshare", { ordered: true });
    setupDC(dc);
    var offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    await signal(remoteId, "offer", { sdp: offer.sdp, type: offer.type, purpose: "pair" });
  }

  async function onOffer(m) {
    var d = m.data || {};
    var purpose = d.purpose || "pair";
    if (purpose === "pair") {
      state.remoteId = m.from;
      state.isOfferer = false;
      var pc = createPC();
      await pc.setRemoteDescription(new RTCSessionDescription({ type: "offer", sdp: d.sdp }));
      flushIce();
      var answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      await signal(m.from, "answer", { sdp: answer.sdp, type: answer.type, purpose: "pair" });
    } else if (purpose === "call") {
      await onCallOffer(m.from, d);
    } else if (purpose === "call-end") {
      await onCallEndOffer(m.from, d);
    }
  }

  async function onAnswer(m) {
    var d = m.data || {};
    if (!state.pc) return;
    await state.pc.setRemoteDescription(new RTCSessionDescription({ type: "answer", sdp: d.sdp }));
    flushIce();
    var purpose = d.purpose || "pair";
    if (purpose === "call" && state.call && state.call.phase === "wait-answer") {
      state.call.phase = "active";
      setCallStatus("Connected");
      startCallTimer();
    }
  }

  async function onIce(m) {
    var cand = m.data;
    if (!cand || !state.pc) return;
    try {
      if (state.pc.remoteDescription) {
        await state.pc.addIceCandidate(new RTCIceCandidate(cand));
      } else {
        state.pendingIce.push(cand);
      }
    } catch (e) { /* ignore stale candidates */ }
  }

  function flushIce() {
    if (!state.pc || !state.pc.remoteDescription) return;
    var q = state.pendingIce;
    state.pendingIce = [];
    q.forEach(function (c) {
      state.pc.addIceCandidate(new RTCIceCandidate(c)).catch(function () {});
    });
  }

  /* ---------------- data channel ---------------- */
  function setupDC(dc) {
    state.dc = dc;
    dc.onopen = function () { onConnected(); };
    dc.onclose = function () { onDisconnected(); };
    dc.onerror = function () {};
    dc.onmessage = function (e) { onDCMessage(e.data); };
  }

  function sendDC(obj) {
    if (state.dc && state.dc.readyState === "open") {
      state.dc.send(JSON.stringify(obj));
      return true;
    }
    return false;
  }

  function onDCMessage(raw) {
    var m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || !m.t) return;
    switch (m.t) {
      case "msg": onChatMsg(m); break;
      case "file-meta": onFileMeta(m); break;
      case "file-chunk": onFileChunk(m); break;
      case "file-end": onFileEnd(m); break;
      case "file-reject": onFileReject(m); break;
      case "call-offer": onIncomingCall(m); break;
      case "call-decline": onCallDeclined(); break;
      case "hangup": onRemoteHangup(); break;
    }
  }

  function onConnected() {
    // One-time code: burn it, mint a fresh one.
    if (state.code) {
      db.ref("codes/" + state.code).remove().catch(function () {});
      state.code = null;
    }
    if (state.pendingReq) { state.pendingReq = null; $("waitingCard").hidden = true; }
    publishCode();
    goOnline();
    if (state.remoteId) watchPeerPresence();
    setStatus("Connected", true);
    $("peerBar").hidden = false;
    $("noPeerPanel").hidden = true;
    $("chatArea").hidden = false;
    $("discBanner").hidden = true;
    showScreen("chat");
    toast("Connected — private session started");
  }

  function onDisconnected() {
    if (!state.remoteId) return;
    setStatus("Disconnected", false);
    setPeerOnline(false);
    $("discBanner").hidden = false;
    if (state.call) endCallUI();
  }

  async function reconnect() {
    if (!state.remoteId) return;
    $("discBanner").hidden = true;
    toast("Reconnecting...");
    try { await startPairingAsOfferer(state.remoteId); }
    catch (e) {
      $("discBanner").hidden = false;
      toast("Reconnect failed. Try again.");
    }
  }

  /* ---------------- presence ---------------- */
  function goOnline() {
    var ref = db.ref("presence/" + state.myId);
    ref.set({ online: true, at: SV() }).catch(function () {});
    try { ref.onDisconnect().remove(); } catch (e) {}
  }

  function watchPeerPresence() {
    if (state.presenceRef) { try { state.presenceRef.off(); } catch (e) {} }
    var ref = db.ref("presence/" + state.remoteId);
    state.presenceRef = ref;
    ref.on("value", function (snap) { setPeerOnline(snap.exists()); });
  }

  function setPeerOnline(on) {
    var txt = $("peerStateText");
    var st = $("peerState");
    if (on) {
      txt.textContent = "Online";
      st.classList.remove("offline");
      st.querySelector(".status-dot").classList.add("on");
    } else {
      txt.textContent = "Offline";
      st.classList.add("offline");
      st.querySelector(".status-dot").classList.remove("on");
    }
  }

  /* ---------------- chat ---------------- */
  function renderMessage(who, text, at) {
    var box = $("messages");
    var d = document.createElement("div");
    d.className = "msg " + who;
    var t = document.createElement("span");
    t.textContent = text;
    d.appendChild(t);
    var time = document.createElement("span");
    time.className = "msg-time";
    time.textContent = fmtTime(at);
    d.appendChild(time);
    box.appendChild(d);
    box.scrollTop = box.scrollHeight;
  }

  function onChatMsg(m) {
    renderMessage("peer", String(m.text || "").slice(0, 2000), m.at);
  }

  function sendMsg() {
    var inp = $("chatInput");
    var text = inp.value.trim();
    if (!text) return;
    if (!sendDC({ t: "msg", id: uid(), text: text.slice(0, 2000), at: Date.now() })) {
      toast("Not connected");
      return;
    }
    renderMessage("own", text.slice(0, 2000), Date.now());
    inp.value = "";
  }

  /* ---------------- files ---------------- */
  function renderFileMessage(who, name, size, id) {
    var box = $("messages");
    var d = document.createElement("div");
    d.className = "msg " + who + " file-bubble";
    var title = document.createElement("p");
    title.className = "file-name";
    title.textContent = name;
    var sz = document.createElement("p");
    sz.className = "file-size";
    sz.textContent = fmtSize(size);
    var track = document.createElement("div");
    track.className = "progress-track";
    var fill = document.createElement("div");
    fill.className = "progress-fill";
    track.appendChild(fill);
    var act = document.createElement("div");
    d.appendChild(title);
    d.appendChild(sz);
    d.appendChild(track);
    d.appendChild(act);
    var time = document.createElement("span");
    time.className = "msg-time";
    time.textContent = fmtTime(Date.now());
    d.appendChild(time);
    box.appendChild(d);
    box.scrollTop = box.scrollHeight;
    state.fileUI[id] = { fill: fill, act: act, box: box };
    return d;
  }

  function updateFileProgress(id, frac) {
    var ui = state.fileUI[id];
    if (ui) ui.fill.style.width = Math.min(100, Math.round(frac * 100)) + "%";
    var box = $("messages");
    box.scrollTop = box.scrollHeight;
  }

  function markFileDone(id, url, name) {
    var ui = state.fileUI[id];
    if (!ui) return;
    ui.fill.style.width = "100%";
    var act = ui.act;
    act.innerHTML = "";
    if (url) {
      var a = document.createElement("a");
      a.className = "file-dl";
      a.href = url;
      a.download = name || "file";
      a.innerHTML = '<svg class="ic ic-sm"><use href="#icon-file"/></svg><span>Download</span>';
      act.appendChild(a);
    } else {
      var s = document.createElement("span");
      s.className = "file-size";
      s.textContent = "Sent";
      act.appendChild(s);
    }
  }

  function markFileFailed(id, msg) {
    var ui = state.fileUI[id];
    if (!ui) return;
    var s = document.createElement("span");
    s.className = "file-size";
    s.style.color = "#ff8a8a";
    s.textContent = msg || "Failed";
    ui.act.innerHTML = "";
    ui.act.appendChild(s);
  }

  async function sendFile(file) {
    if (!state.dc || state.dc.readyState !== "open") { toast("Not connected"); return; }
    if (file.size > MAX_FILE) { toast("File too large — 100 MB max."); return; }
    if (file.size === 0) { toast("Empty file."); return; }
    var id = uid();
    var total = Math.ceil(file.size / CHUNK);
    renderFileMessage("own", file.name, file.size, id);
    try {
      if (!sendDC({ t: "file-meta", id: id, name: file.name, size: file.size, type: file.type || "application/octet-stream", total: total })) {
        throw new Error("not-connected");
      }
      var buf = await file.arrayBuffer();
      for (var n = 0; n < total; n++) {
        if (!state.dc || state.dc.readyState !== "open") throw new Error("disconnected");
        while (state.dc.bufferedAmount > 2 * 1024 * 1024) {
          await sleep(120);
          if (!state.dc || state.dc.readyState !== "open") throw new Error("disconnected");
        }
        var slice = buf.slice(n * CHUNK, (n + 1) * CHUNK);
        state.dc.send(JSON.stringify({ t: "file-chunk", id: id, n: n, total: total, buf: base64Encode(slice) }));
        updateFileProgress(id, (n + 1) / total);
      }
      sendDC({ t: "file-end", id: id });
      markFileDone(id, null);
      toast("File sent");
    } catch (e) {
      markFileFailed(id, "Send failed");
    }
  }

  function onFileMeta(m) {
    if (!m.id || !(m.size >= 0)) return;
    if (m.size > MAX_FILE) {
      sendDC({ t: "file-reject", id: m.id });
      toast("File too large — declined automatically.");
      return;
    }
    state.incomingFiles[m.id] = {
      name: String(m.name || "file").slice(0, 200),
      size: m.size,
      type: String(m.type || "application/octet-stream").slice(0, 100),
      total: m.total || 0,
      chunks: [],
      received: 0
    };
    renderFileMessage("peer", state.incomingFiles[m.id].name, m.size, m.id);
  }

  function onFileChunk(m) {
    var f = state.incomingFiles[m.id];
    if (!f || m.n == null || m.n >= f.total) return;
    if (!f.chunks[m.n]) {
      try { f.chunks[m.n] = base64Decode(m.buf || ""); }
      catch (e) { return; }
      f.received++;
      updateFileProgress(m.id, f.received / f.total);
    }
  }

  function onFileEnd(m) {
    var f = state.incomingFiles[m.id];
    if (!f) return;
    delete state.incomingFiles[m.id];
    try {
      var blob = new Blob(f.chunks, { type: f.type });
      var url = URL.createObjectURL(blob);
      markFileDone(m.id, url, f.name);
      toast("File received");
    } catch (e) {
      markFileFailed(m.id, "Could not assemble file");
    }
  }

  function onFileReject(m) {
    markFileFailed(m.id, "Declined by peer");
  }

  /* ---------------- calls ---------------- */
  function showCallUI(media, phase) {
    $("callOverlay").hidden = false;
    $("incomingCard").hidden = true;
    $("callControls").hidden = false;
    var isVideo = media === "video";
    $("voiceAvatar").hidden = isVideo;
    $("remoteVideo").style.visibility = isVideo ? "visible" : "hidden";
    $("selfVideo").style.display = isVideo ? "block" : "none";
    $("btnCam").style.display = isVideo ? "inline-flex" : "none";
    setCallStatus(phase === "calling" ? "Calling..." : phase === "ringing" ? "Ringing..." : "Connecting...");
    setMicIcon(true);
    setCamIcon(true);
    state.micOn = true;
    state.camOn = true;
    state.speakerOn = true;
    $("btnSpeaker").classList.remove("off");
  }

  function setCallStatus(s) { $("voiceState").textContent = s; }

  function startCallTimer() {
    stopCallTimer();
    state.callStart = Date.now();
    state.callTimer = setInterval(function () {
      var s = Math.floor((Date.now() - state.callStart) / 1000);
      var mm = Math.floor(s / 60), ss = s % 60;
      setCallStatus("Connected · " + (mm < 10 ? "0" + mm : mm) + ":" + (ss < 10 ? "0" + ss : ss));
    }, 1000);
  }

  function stopCallTimer() {
    if (state.callTimer) { clearInterval(state.callTimer); state.callTimer = null; }
  }

  function hideCallOverlay() {
    $("callOverlay").hidden = true;
    $("incomingCard").hidden = true;
    $("callControls").hidden = true;
    stopCallTimer();
  }

  function addTracksToPC(stream) {
    state.localStream = stream;
    var sv = $("selfVideo");
    if (sv.srcObject !== stream) sv.srcObject = stream;
    stream.getTracks().forEach(function (tr) {
      try { state.pc.addTrack(tr, stream); } catch (e) {}
    });
  }

  function cleanupCallMedia() {
    if (state.localStream) {
      state.localStream.getTracks().forEach(function (t) { try { t.stop(); } catch (e) {} });
      state.localStream = null;
    }
    var sv = $("selfVideo");
    if (sv.srcObject) { sv.srcObject = null; }
    var rv = $("remoteVideo");
    if (rv.srcObject) { rv.srcObject = null; }
  }

  async function startCall(media) {
    if (!state.dc || state.dc.readyState !== "open") { toast("Not connected"); return; }
    if (state.call) { toast("Already in a call"); return; }
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      toast("Calls are not supported in this browser.");
      return;
    }
    state.call = { media: media, dir: "out", phase: "calling" };
    showCallUI(media, "calling");
    if (!sendDC({ t: "call-offer", media: media })) {
      endCallUI();
      toast("Not connected");
      return;
    }
  }

  function onIncomingCall(m) {
    if (state.call) {
      sendDC({ t: "call-decline" });
      return;
    }
    var media = m.media === "video" ? "video" : "audio";
    state.call = { media: media, dir: "in", phase: "ringing" };
    $("callOverlay").hidden = false;
    $("callControls").hidden = true;
    $("voiceAvatar").hidden = true;
    $("remoteVideo").style.visibility = "hidden";
    $("incomingCard").hidden = false;
    $("incomingTitle").textContent = "Incoming call";
    $("incomingSub").textContent = (media === "video" ? "Video" : "Voice") + " call from connected peer";
  }

  async function acceptCall() {
    if (!state.call || state.call.dir !== "in") return;
    var media = state.call.media;
    var stream;
    try {
      stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: media === "video" });
    } catch (e) {
      toast("Camera/microphone permission denied.");
      sendDC({ t: "call-decline" });
      endCallUI();
      return;
    }
    state.call.phase = "wait-answer";
    showCallUI(media, "connecting");
    setCallStatus("Connecting...");
    addTracksToPC(stream);
    try {
      var offer = await state.pc.createOffer();
      await state.pc.setLocalDescription(offer);
      await signal(state.remoteId, "offer", { sdp: offer.sdp, type: offer.type, purpose: "call", media: media });
    } catch (e) {
      toast("Call setup failed.");
      endCallUI();
    }
  }

  function declineCall() {
    sendDC({ t: "call-decline" });
    endCallUI();
    toast("Call declined");
  }

  async function onCallOffer(from, d) {
    // Callee accepted and sent SDP (we are the caller).
    if (!state.call || state.call.dir !== "out") return;
    var media = (d && d.media) || state.call.media;
    var stream;
    try {
      stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: media === "video" });
    } catch (e) {
      toast("Camera/microphone permission denied.");
      sendDC({ t: "hangup" });
      endCallUI();
      return;
    }
    addTracksToPC(stream);
    try {
      await state.pc.setRemoteDescription(new RTCSessionDescription({ type: "offer", sdp: d.sdp }));
      flushIce();
      var answer = await state.pc.createAnswer();
      await state.pc.setLocalDescription(answer);
      await signal(from, "answer", { sdp: answer.sdp, type: answer.type, purpose: "call" });
      state.call.phase = "active";
      showCallUI(media, "active");
      setCallStatus("Connected");
      startCallTimer();
    } catch (e) {
      toast("Call setup failed.");
      endCallUI();
    }
  }

  function onCallDeclined() {
    if (state.call && state.call.dir === "out") {
      endCallUI();
      toast("Call declined");
    }
  }

  async function hangup() {
    if (!state.call) { hideCallOverlay(); return; }
    sendDC({ t: "hangup" });
    await endCallCleanup(true);
  }

  async function onRemoteHangup() {
    if (!state.call) { hideCallOverlay(); return; }
    toast("Call ended");
    await endCallCleanup(false);
  }

  async function endCallCleanup(renegotiate) {
    var hadPc = !!(state.pc && state.remoteId);
    state.call = null;
    cleanupCallMedia();
    hideCallOverlay();
    if (renegotiate && hadPc && state.pc.signalingState === "stable") {
      try {
        var senders = state.pc.getSenders();
        for (var i = 0; i < senders.length; i++) {
          try { state.pc.removeTrack(senders[i]); } catch (e) {}
        }
        var offer = await state.pc.createOffer();
        await state.pc.setLocalDescription(offer);
        await signal(state.remoteId, "offer", { sdp: offer.sdp, type: offer.type, purpose: "call-end" });
      } catch (e) { /* best effort */ }
    }
  }

  function endCallUI() {
    state.call = null;
    cleanupCallMedia();
    hideCallOverlay();
  }

  async function onCallEndOffer(from, d) {
    try {
      await state.pc.setRemoteDescription(new RTCSessionDescription({ type: "offer", sdp: d.sdp }));
      flushIce();
      var answer = await state.pc.createAnswer();
      await state.pc.setLocalDescription(answer);
      await signal(from, "answer", { sdp: answer.sdp, type: answer.type, purpose: "call-end" });
    } catch (e) { /* best effort */ }
  }

  function setMicIcon(on) {
    var b = $("btnMic");
    b.classList.toggle("off", !on);
    b.querySelector("use").setAttribute("href", on ? "#icon-mic" : "#icon-mic-off");
  }

  function setCamIcon(on) {
    var b = $("btnCam");
    b.classList.toggle("off", !on);
    b.querySelector("use").setAttribute("href", on ? "#icon-camera" : "#icon-camera-off");
  }

  function toggleMic() {
    if (!state.localStream) return;
    state.micOn = !state.micOn;
    state.localStream.getAudioTracks().forEach(function (t) { t.enabled = state.micOn; });
    setMicIcon(state.micOn);
  }

  function toggleCam() {
    if (!state.localStream) return;
    state.camOn = !state.camOn;
    state.localStream.getVideoTracks().forEach(function (t) { t.enabled = state.camOn; });
    setCamIcon(state.camOn);
  }

  function toggleSpeaker() {
    state.speakerOn = !state.speakerOn;
    $("remoteVideo").muted = !state.speakerOn;
    $("btnSpeaker").classList.toggle("off", !state.speakerOn);
  }

  /* ---------------- header status ---------------- */
  function setStatus(text, on) {
    $("statusText").textContent = text;
    $("statusDot").classList.toggle("on", !!on);
  }

  /* ---------------- screens / tabs ---------------- */
  function showScreen(name) {
    ["pair", "requests", "chat"].forEach(function (s) {
      var sec = $("screen-" + s);
      var active = s === name;
      sec.classList.toggle("active", active);
      if (active) sec.removeAttribute("hidden"); else sec.setAttribute("hidden", "");
    });
    [["tabPair", "pair"], ["tabRequests", "requests"], ["tabChat", "chat"]].forEach(function (p) {
      $(p[0]).classList.toggle("active", p[1] === name);
    });
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  /* ---------------- init ---------------- */
  function fatal(msg) {
    document.querySelector(".container").innerHTML =
      '<div class="glass-panel"><h2 class="panel-title">Setup needed</h2>' +
      '<p class="panel-sub">' + esc(msg) + "</p></div>";
  }

  function handleInviteParam() {
    try {
      var j = new URLSearchParams(location.search).get("j");
      if (j) {
        var code = String(j).toUpperCase().replace(/[^A-Z2-9]/g, "").slice(0, 6);
        if (code.length === 6) {
          $("codeInput").value = code;
          toast("Invite code filled — tap Connect");
        }
      }
    } catch (e) {}
  }

  function init() {
    if (typeof firebase === "undefined" || typeof firebaseConfig === "undefined" ||
        !firebaseConfig || firebaseConfig.apiKey === "PASTE_YOURS") {
      fatal("Firebase is not configured. Add your web app config to js/firebase-config.js and enable Realtime Database.");
      return;
    }
    try {
      firebase.initializeApp(firebaseConfig);
      db = firebase.database();
    } catch (e) {
      fatal("Could not start Firebase: " + (e && e.message ? e.message : e));
      return;
    }

    state.myId = getPeerId();

    // Tabs
    $("tabPair").addEventListener("click", function () { showScreen("pair"); });
    $("tabRequests").addEventListener("click", function () { showScreen("requests"); });
    $("tabChat").addEventListener("click", function () { showScreen("chat"); });

    // Pairing
    $("btnCopyCode").addEventListener("click", function () {
      if (state.code) copyText(state.code, "Code copied");
    });
    $("btnCopyLink").addEventListener("click", function () {
      if (state.code) copyText(inviteLink(), "Invite link copied");
    });
    $("btnConnect").addEventListener("click", connect);
    $("codeInput").addEventListener("keydown", function (e) {
      if (e.key === "Enter") connect();
    });
    $("btnCancelReq").addEventListener("click", cancelRequest);
    $("btnGoRequests").addEventListener("click", function () { showScreen("requests"); });

    // Chat
    $("btnSend").addEventListener("click", sendMsg);
    $("chatInput").addEventListener("keydown", function (e) {
      if (e.key === "Enter") sendMsg();
    });
    $("btnAttach").addEventListener("click", function () { $("fileInput").click(); });
    $("fileInput").addEventListener("change", function () {
      var f = $("fileInput").files[0];
      $("fileInput").value = "";
      if (f) sendFile(f);
    });
    $("btnGoPair").addEventListener("click", function () { showScreen("pair"); });
    $("btnReconnect").addEventListener("click", reconnect);

    // Calls
    $("btnVoiceCall").addEventListener("click", function () { startCall("audio"); });
    $("btnVideoCall").addEventListener("click", function () { startCall("video"); });
    $("btnAcceptCall").addEventListener("click", acceptCall);
    $("btnDeclineCall").addEventListener("click", declineCall);
    $("btnHangup").addEventListener("click", hangup);
    $("btnMic").addEventListener("click", toggleMic);
    $("btnCam").addEventListener("click", toggleCam);
    $("btnSpeaker").addEventListener("click", toggleSpeaker);

    watchInbox();
    watchSignals();
    goOnline();
    publishCode();
    setInterval(tickCodeTimer, 1000);
    handleInviteParam();
    renderRequests();

    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("sw.js").catch(function () {});
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();
