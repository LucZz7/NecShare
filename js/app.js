/* ============================================================
   NecShare v2 — private peer-to-peer files, chat and calls.
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

  function lsGet(k, fb) {
    try { var v = localStorage.getItem(k); return v ? JSON.parse(v) : fb; }
    catch (e) { return fb; }
  }
  function lsSet(k, v) {
    try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {}
  }

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

  function fmtDur(sec) {
    sec = Math.max(0, Math.round(sec || 0));
    var m = Math.floor(sec / 60), s = sec % 60;
    return (m < 10 ? "0" + m : m) + ":" + (s < 10 ? "0" + s : s);
  }

  function fmtAgo(ts) {
    var d = Date.now() - (ts || 0);
    if (d < 60 * 1000) return "just now";
    if (d < 60 * 60 * 1000) return Math.floor(d / 60000) + "m ago";
    if (d < 24 * 60 * 60 * 1000) return Math.floor(d / 3600000) + "h ago";
    return Math.floor(d / 86400000) + "d ago";
  }

  function fmtSpeed(bps) {
    if (!bps || bps <= 0) return "--";
    if (bps < 1024) return Math.round(bps) + " B/s";
    if (bps < 1024 * 1024) return (bps / 1024).toFixed(1) + " KB/s";
    return (bps / (1024 * 1024)).toFixed(1) + " MB/s";
  }

  function fmtETA(sec) {
    if (!isFinite(sec) || sec <= 0) return "--";
    sec = Math.round(sec);
    if (sec < 60) return sec + "s left";
    return Math.floor(sec / 60) + "m " + (sec % 60) + "s left";
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

  function isImageType(t) { return /^image\//i.test(t || ""); }

  function makeThumb(blob, cb) {
    // Small JPEG data-URL thumbnail for history (images only).
    try {
      var url = URL.createObjectURL(blob);
      var img = new Image();
      img.onload = function () {
        try {
          var s = 96, r = Math.min(1, s / Math.max(img.width || 1, img.height || 1));
          var c = document.createElement("canvas");
          c.width = Math.max(1, Math.round(img.width * r));
          c.height = Math.max(1, Math.round(img.height * r));
          c.getContext("2d").drawImage(img, 0, 0, c.width, c.height);
          cb(c.toDataURL("image/jpeg", 0.7));
        } catch (e) { cb(null); }
        URL.revokeObjectURL(url);
      };
      img.onerror = function () { URL.revokeObjectURL(url); cb(null); };
      img.src = url;
    } catch (e) { cb(null); }
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
    incomingFiles: {},     // fileId -> {name,size,type,total,chunks,received,voice,dur,rec}
    transfers: {},         // fileId -> transfer record
    fileUI: {},            // fileId -> chat bubble {fill, act, voice, dur, localUrl}
    localStream: null,
    call: null,            // {media:'audio'|'video', dir:'in'|'out', phase, logged}
    callTimer: null,
    callStart: 0,
    presenceRef: null,
    micOn: true,
    camOn: true,
    speakerOn: true,
    recState: null,        // voice recording {mr, chunks, stream, startAt, timer}
    attachForChat: false
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
    renderQR();
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

  function renderQR() {
    var card = $("qrCard");
    try {
      if (typeof QRCode === "undefined" || !state.code) { card.hidden = true; return; }
      card.hidden = false;
      QRCode.toCanvas($("qrCanvas"), inviteLink(),
        { width: 180, margin: 1, color: { dark: "#0a0a0f", light: "#ffffff" } },
        function (err) { if (err) card.hidden = true; });
    } catch (e) { card.hidden = true; }
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
      await sendPairRequest(owner, "Request sent to code " + code + ". Waiting for approval...");
    } catch (e) {
      showError("connectError", (e && e.message) || "Could not send the request.");
    }
    btn.disabled = false;
  }

  async function sendPairRequest(peerId, waitingMsg) {
    var reqRef = await db.ref("peers/" + peerId + "/inbox").push({ from: state.myId, at: SV() });
    state.pendingReq = { to: peerId, reqId: reqRef.key };
    $("waitingCard").hidden = false;
    $("waitingSub").textContent = waitingMsg || "Waiting for the other person to accept...";
  }

  async function connectToPeer(peerId) {
    if (!peerId || peerId === state.myId) return;
    if (state.remoteId) { toast("Already connected"); return; }
    showScreen("connect");
    try {
      await sendPairRequest(peerId, "Request sent to saved device. Waiting for approval...");
      toast("Reconnect request sent");
    } catch (e) {
      toast("Could not send the request.");
    }
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
    var badge = $("tabBadge");
    badge.hidden = n === 0;
    badge.textContent = n;
    $("reqEmpty").hidden = n !== 0;
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
      case "file-cancel": onFileCancel(m); break;
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
    saveDevice(state.remoteId);
    publishCode();
    goOnline();
    if (state.remoteId) watchPeerPresence();
    setStatus("Connected", true);
    $("peerBar").hidden = false;
    $("noPeerPanel").hidden = true;
    $("chatArea").hidden = false;
    $("noPeerFiles").hidden = true;
    $("noPeerCalls").hidden = true;
    $("callsArea").hidden = false;
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

  /* ---------------- transfer engine (FILES tab) ---------------- */
  function newTransfer(id, dir, name, size, type, extra) {
    extra = extra || {};
    var rec = {
      id: id,
      dir: dir, // 'out' | 'in'
      name: String(name || "file").slice(0, 200),
      size: size || 0,
      type: String(type || "application/octet-stream").slice(0, 100),
      voice: !!extra.voice,
      dur: extra.dur || 0,
      status: "active",
      bytes: 0,
      startAt: Date.now(),
      lastT: Date.now(),
      lastBytes: 0,
      speed: 0,
      cancelled: false,
      histThumb: null,
      localUrl: extra.localUrl || null,
      ui: null
    };
    state.transfers[id] = rec;
    renderTransferCard(rec);
    $("activePanel").hidden = false;
    return rec;
  }

  function thumbIcon(rec) {
    if (rec.voice) return "icon-mic";
    if (isImageType(rec.type)) return "icon-image";
    return "icon-file";
  }

  function renderTransferCard(rec) {
    var list = $("transferList");
    var card = document.createElement("div");
    card.className = "t-card";
    card.id = "tc-" + rec.id;

    var thumb = document.createElement("div");
    thumb.className = "t-thumb";
    thumb.innerHTML = '<svg class="ic"><use href="#' + thumbIcon(rec) + '"/></svg>';

    var meta = document.createElement("div");
    meta.className = "t-meta";
    var nm = document.createElement("p");
    nm.className = "t-name";
    nm.textContent = rec.name;
    var sub = document.createElement("p");
    sub.className = "t-sub";
    var dirSpan = document.createElement("span");
    dirSpan.className = "t-dir " + (rec.dir === "out" ? "up" : "down");
    dirSpan.innerHTML = '<svg class="ic ic-xs"><use href="#' + (rec.dir === "out" ? "icon-up" : "icon-down") + '"/></svg>' +
      (rec.dir === "out" ? "Sending" : "Receiving");
    var sizeSpan = document.createElement("span");
    sizeSpan.textContent = fmtSize(rec.size);
    var stats = document.createElement("span");
    stats.className = "t-stats";
    sub.appendChild(dirSpan);
    sub.appendChild(sizeSpan);
    sub.appendChild(stats);

    var track = document.createElement("div");
    track.className = "progress-track";
    var fill = document.createElement("div");
    fill.className = "progress-fill";
    track.appendChild(fill);
    meta.appendChild(nm);
    meta.appendChild(sub);
    meta.appendChild(track);

    var cancel = document.createElement("button");
    cancel.className = "t-cancel";
    cancel.setAttribute("aria-label", "Cancel transfer");
    cancel.innerHTML = '<svg class="ic ic-sm"><use href="#icon-x"/></svg>';
    cancel.addEventListener("click", function () { cancelTransfer(rec.id); });

    card.appendChild(thumb);
    card.appendChild(meta);
    card.appendChild(cancel);
    list.appendChild(card);
    rec.ui = { card: card, thumb: thumb, fill: fill, stats: stats, cancel: cancel, dirSpan: dirSpan };
  }

  function setCardThumb(rec, url) {
    if (!rec.ui || !url) return;
    var img = document.createElement("img");
    img.className = "t-thumb";
    img.src = url;
    img.alt = "";
    rec.ui.thumb.replaceWith(img);
    rec.ui.thumb = img;
  }

  function updateTransferProgress(rec, bytesDone) {
    if (!rec || rec.status !== "active") return;
    rec.bytes = Math.min(rec.size, bytesDone);
    var now = Date.now();
    var dt = (now - rec.lastT) / 1000;
    if (dt >= 0.4) {
      var inst = (rec.bytes - rec.lastBytes) / dt;
      rec.speed = rec.speed ? rec.speed * 0.6 + inst * 0.4 : inst;
      rec.lastT = now;
      rec.lastBytes = rec.bytes;
    }
    var frac = rec.size ? rec.bytes / rec.size : 0;
    if (rec.ui) {
      rec.ui.fill.style.width = Math.min(100, Math.round(frac * 100)) + "%";
      var eta = rec.speed > 10 ? (rec.size - rec.bytes) / rec.speed : 0;
      rec.ui.stats.textContent = fmtSpeed(rec.speed) + " · " + fmtETA(eta);
    }
    var fui = state.fileUI[rec.id];
    if (fui && fui.fill) {
      fui.fill.style.width = Math.min(100, Math.round(frac * 100)) + "%";
      var box = $("messages");
      box.scrollTop = box.scrollHeight;
    }
  }

  function completeTransfer(rec, blobUrl) {
    if (!rec || rec.status !== "active") return;
    rec.status = "done";
    rec.bytes = rec.size;
    if (rec.ui) {
      rec.ui.fill.style.width = "100%";
      rec.ui.stats.textContent = "Done · " + fmtSpeed(rec.size / Math.max(0.5, (Date.now() - rec.startAt) / 1000)) + " avg";
      var done = document.createElement("span");
      done.className = "t-done-label";
      if (rec.dir === "in" && blobUrl) {
        var a = document.createElement("a");
        a.className = "file-dl";
        a.href = blobUrl;
        a.download = rec.name;
        a.innerHTML = '<svg class="ic ic-sm"><use href="#icon-file"/></svg><span>Download</span>';
        done.appendChild(a);
      } else {
        done.innerHTML = '<svg class="ic ic-sm"><use href="#icon-check"/></svg><span>Done</span>';
      }
      rec.ui.cancel.replaceWith(done);
    }
    // Chat bubble side
    var fui = state.fileUI[rec.id];
    if (fui) markChatFileDone(rec, fui, blobUrl);
    addHistory(rec);
    if (rec.dir === "in") toast(rec.voice ? "Voice message received" : "File received");
    else if (!rec.voice) toast("File sent");
  }

  function failTransfer(rec, msg) {
    if (!rec || rec.status !== "active") return;
    rec.status = "failed";
    if (rec.ui) {
      rec.ui.stats.textContent = msg || "Failed";
      rec.ui.stats.style.color = "#ff8a8a";
      var s = document.createElement("span");
      s.className = "t-done-label";
      s.textContent = msg || "Failed";
      s.style.color = "#ff8a8a";
      rec.ui.cancel.replaceWith(s);
    }
    var fui = state.fileUI[rec.id];
    if (fui) {
      fui.act.innerHTML = "";
      var sp = document.createElement("span");
      sp.className = "file-size";
      sp.style.color = "#ff8a8a";
      sp.textContent = msg || "Failed";
      fui.act.appendChild(sp);
    }
  }

  function cancelTransfer(id) {
    var rec = state.transfers[id];
    if (!rec || rec.status !== "active") return;
    rec.cancelled = true;
    sendDC({ t: "file-cancel", id: id });
    if (rec.dir === "in") delete state.incomingFiles[id];
    failTransfer(rec, "Cancelled");
    toast("Transfer cancelled");
  }

  function onFileCancel(m) {
    var rec = state.transfers[m.id];
    if (rec && rec.status === "active") {
      if (rec.dir === "in") delete state.incomingFiles[m.id];
      failTransfer(rec, "Cancelled by peer");
    }
  }

  /* ---------------- transfer history ---------------- */
  function addHistory(rec) {
    var h = lsGet("necshare_history_v1", []);
    h.unshift({
      tid: rec.id,
      name: rec.name, size: rec.size, type: rec.type,
      dir: rec.dir, at: Date.now(), voice: rec.voice,
      dur: rec.dur, thumb: rec.histThumb || null
    });
    lsSet("necshare_history_v1", h.slice(0, 40));
    renderHistory();
  }

  function updateHistoryThumb(tid, thumb) {
    if (!thumb) return;
    var h = lsGet("necshare_history_v1", []);
    var changed = false;
    for (var i = 0; i < h.length; i++) {
      if (h[i].tid === tid && !h[i].thumb) { h[i].thumb = thumb; changed = true; break; }
    }
    if (changed) { lsSet("necshare_history_v1", h); renderHistory(); }
  }

  function renderHistory() {
    var list = $("historyList");
    var h = lsGet("necshare_history_v1", []);
    $("historyEmpty").hidden = h.length !== 0;
    list.innerHTML = "";
    h.forEach(function (e) {
      var row = document.createElement("div");
      row.className = "h-row";
      var th = document.createElement("div");
      th.className = "h-thumb";
      if (e.thumb) {
        var img = document.createElement("img");
        img.className = "h-thumb";
        img.src = e.thumb;
        img.alt = "";
        th.replaceWith(img);
        th = img;
      } else {
        th.innerHTML = '<svg class="ic ic-sm"><use href="#' +
          (e.voice ? "icon-mic" : isImageType(e.type) ? "icon-image" : "icon-file") + '"/></svg>';
      }
      var meta = document.createElement("div");
      meta.className = "h-meta";
      var nm = document.createElement("p");
      nm.className = "h-name";
      nm.textContent = e.name;
      var sub = document.createElement("p");
      sub.className = "h-sub";
      sub.textContent = (e.dir === "out" ? "Sent" : "Received") + " · " + fmtSize(e.size) +
        (e.voice && e.dur ? " · " + fmtDur(e.dur) : "") + " · " + fmtAgo(e.at);
      meta.appendChild(nm);
      meta.appendChild(sub);
      var dir = document.createElement("span");
      dir.className = "t-dir " + (e.dir === "out" ? "up" : "down");
      dir.innerHTML = '<svg class="ic ic-sm"><use href="#' + (e.dir === "out" ? "icon-up" : "icon-down") + '"/></svg>';
      row.appendChild(th);
      row.appendChild(meta);
      row.appendChild(dir);
      list.appendChild(row);
    });
  }

  function clearHistory() {
    lsSet("necshare_history_v1", []);
    renderHistory();
    toast("History cleared");
  }

  /* ---------------- files: send / receive ---------------- */
  async function sendFile(file, opts) {
    opts = opts || {};
    if (!state.dc || state.dc.readyState !== "open") { toast("Not connected"); return; }
    if (file.size > MAX_FILE) { toast("File too large — 100 MB max."); return; }
    if (file.size === 0) { toast("Empty file."); return; }
    var id = uid();
    var total = Math.max(1, Math.ceil(file.size / CHUNK));
    var rec = newTransfer(id, "out", file.name, file.size, file.type,
      { voice: opts.voice, dur: opts.dur, localUrl: opts.voice ? URL.createObjectURL(file) : null });
    if (opts.chat) {
      var bubble = renderFileMessage("own", file.name, file.size, id);
      state.fileUI[id] = { fill: bubble.fill, act: bubble.act, voice: !!opts.voice, dur: opts.dur || 0, localUrl: rec.localUrl };
    }
    if (isImageType(file.type)) {
      try { setCardThumb(rec, URL.createObjectURL(file)); } catch (e) {}
      makeThumb(file, function (t) { rec.histThumb = t; updateHistoryThumb(rec.id, t); });
    }
    try {
      if (!sendDC({
        t: "file-meta", id: id, name: file.name, size: file.size,
        type: file.type || "application/octet-stream", total: total,
        voice: !!opts.voice, dur: opts.dur || 0
      })) throw new Error("not-connected");
      var buf = await file.arrayBuffer();
      for (var n = 0; n < total; n++) {
        if (rec.cancelled) throw { cancelled: true };
        if (!state.dc || state.dc.readyState !== "open") throw new Error("disconnected");
        while (state.dc.bufferedAmount > 2 * 1024 * 1024) {
          await sleep(120);
          if (rec.cancelled) throw { cancelled: true };
          if (!state.dc || state.dc.readyState !== "open") throw new Error("disconnected");
        }
        var slice = buf.slice(n * CHUNK, (n + 1) * CHUNK);
        state.dc.send(JSON.stringify({ t: "file-chunk", id: id, n: n, total: total, buf: base64Encode(slice) }));
        updateTransferProgress(rec, Math.min(file.size, (n + 1) * CHUNK));
      }
      sendDC({ t: "file-end", id: id });
      completeTransfer(rec, null);
    } catch (e) {
      if (!(e && e.cancelled)) failTransfer(rec, "Send failed");
    }
  }

  function onFileMeta(m) {
    if (!m.id || !(m.size >= 0)) return;
    if (m.size > MAX_FILE) {
      sendDC({ t: "file-reject", id: m.id });
      toast("File too large — declined automatically.");
      return;
    }
    var voice = !!m.voice;
    state.incomingFiles[m.id] = {
      name: String(m.name || "file").slice(0, 200),
      size: m.size,
      type: String(m.type || "application/octet-stream").slice(0, 100),
      total: m.total || 0,
      chunks: [],
      received: 0,
      voice: voice,
      dur: m.dur || 0
    };
    var rec = newTransfer(m.id, "in", state.incomingFiles[m.id].name, m.size,
      state.incomingFiles[m.id].type, { voice: voice, dur: m.dur || 0 });
    var bubble = renderFileMessage("peer", rec.name, m.size, m.id);
    state.fileUI[m.id] = { fill: bubble.fill, act: bubble.act, voice: voice, dur: m.dur || 0, localUrl: null };
  }

  function onFileChunk(m) {
    var f = state.incomingFiles[m.id];
    if (!f || m.n == null || m.n >= f.total) return;
    if (!f.chunks[m.n]) {
      try { f.chunks[m.n] = base64Decode(m.buf || ""); }
      catch (e) { return; }
      f.received++;
      var rec = state.transfers[m.id];
      if (rec) updateTransferProgress(rec, Math.min(f.size, f.received * CHUNK));
    }
  }

  function onFileEnd(m) {
    var f = state.incomingFiles[m.id];
    if (!f) return;
    delete state.incomingFiles[m.id];
    var rec = state.transfers[m.id];
    try {
      var blob = new Blob(f.chunks, { type: f.type });
      var url = URL.createObjectURL(blob);
      if (rec) {
        if (isImageType(f.type)) {
          setCardThumb(rec, url);
          makeThumb(blob, function (t) { rec.histThumb = t; updateHistoryThumb(rec.id, t); });
        }
        var fui = state.fileUI[m.id];
        if (fui) fui.localUrl = url;
        completeTransfer(rec, url);
      }
    } catch (e) {
      if (rec) failTransfer(rec, "Could not assemble file");
    }
  }

  function onFileReject(m) {
    var rec = state.transfers[m.id];
    if (rec) failTransfer(rec, "Declined by peer");
  }

  function markChatFileDone(rec, fui, blobUrl) {
    fui.fill.style.width = "100%";
    fui.act.innerHTML = "";
    if (fui.voice) {
      var url = fui.localUrl || blobUrl;
      if (url) {
        var au = document.createElement("audio");
        au.controls = true;
        au.src = url;
        fui.act.appendChild(au);
        if (fui.dur) {
          var d = document.createElement("p");
          d.className = "voice-dur";
          d.textContent = "Voice message · " + fmtDur(fui.dur);
          fui.act.appendChild(d);
        }
      }
    } else if (blobUrl) {
      var a = document.createElement("a");
      a.className = "file-dl";
      a.href = blobUrl;
      a.download = rec.name;
      a.innerHTML = '<svg class="ic ic-sm"><use href="#icon-file"/></svg><span>Download</span>';
      fui.act.appendChild(a);
    } else {
      var s = document.createElement("span");
      s.className = "file-size";
      s.textContent = "Sent";
      fui.act.appendChild(s);
    }
    var box = $("messages");
    box.scrollTop = box.scrollHeight;
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
    return { fill: fill, act: act };
  }

  /* ---------------- voice messages ---------------- */
  function toggleVoiceRec() {
    if (state.recState) stopVoiceRec();
    else startVoiceRec();
  }

  async function startVoiceRec() {
    if (!state.dc || state.dc.readyState !== "open") { toast("Not connected"); return; }
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      toast("Voice messages are not supported in this browser.");
      return;
    }
    if (typeof MediaRecorder === "undefined") {
      toast("Voice messages are not supported in this browser.");
      return;
    }
    var stream;
    try {
      stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    } catch (e) {
      toast("Microphone permission denied.");
      return;
    }
    var mime = "";
    try {
      if (MediaRecorder.isTypeSupported("audio/webm")) mime = "audio/webm";
    } catch (e) {}
    var mr;
    try {
      mr = mime ? new MediaRecorder(stream, { mimeType: mime }) : new MediaRecorder(stream);
    } catch (e) {
      stream.getTracks().forEach(function (t) { try { t.stop(); } catch (e2) {} });
      toast("Could not start recording.");
      return;
    }
    var chunks = [];
    mr.ondataavailable = function (e) { if (e.data && e.data.size) chunks.push(e.data); };
    try { mr.start(); } catch (e) {
      stream.getTracks().forEach(function (t) { try { t.stop(); } catch (e2) {} });
      toast("Could not start recording.");
      return;
    }
    var startAt = Date.now();
    var timer = setInterval(function () {
      $("recTime").textContent = fmtDur((Date.now() - startAt) / 1000);
    }, 500);
    state.recState = { mr: mr, chunks: chunks, stream: stream, startAt: startAt, timer: timer };
    $("recBar").hidden = false;
    $("recTime").textContent = "00:00";
    $("btnVoice").classList.add("recording");
  }

  function stopVoiceRec() {
    var rs = state.recState;
    if (!rs) return;
    state.recState = null;
    clearInterval(rs.timer);
    $("recBar").hidden = true;
    $("btnVoice").classList.remove("recording");
    var dur = Math.max(1, Math.round((Date.now() - rs.startAt) / 1000));
    rs.mr.onstop = function () {
      try { rs.stream.getTracks().forEach(function (t) { t.stop(); }); } catch (e) {}
      var blob = new Blob(rs.chunks, { type: rs.mr.mimeType || "audio/webm" });
      if (blob.size < 300) { toast("Recording too short"); return; }
      var f = new File([blob], "Voice message.webm", { type: blob.type || "audio/webm" });
      sendFile(f, { chat: true, voice: true, dur: dur });
    };
    try { rs.mr.stop(); } catch (e) {}
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
      setCallStatus("Connected · " + fmtDur(s));
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
    state.call = { media: media, dir: "out", phase: "calling", logged: false };
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
    state.call = { media: media, dir: "in", phase: "ringing", logged: false };
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

  function finishCallLog() {
    if (state.call && !state.call.logged) {
      state.call.logged = true;
      var dur = state.callStart ? Math.round((Date.now() - state.callStart) / 1000) : 0;
      logCall(state.call.media, state.call.dir, Math.max(0, dur));
    }
  }

  async function endCallCleanup(renegotiate) {
    finishCallLog();
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
    finishCallLog();
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

  /* ---------------- recent calls log ---------------- */
  function logCall(type, dir, dur) {
    var log = lsGet("necshare_calls_v1", []);
    log.unshift({ type: type === "video" ? "video" : "audio", dir: dir, dur: dur, at: Date.now() });
    lsSet("necshare_calls_v1", log.slice(0, 30));
    renderCallLog();
  }

  function renderCallLog() {
    var list = $("callLogList");
    var log = lsGet("necshare_calls_v1", []);
    $("callLogEmpty").hidden = log.length !== 0;
    list.innerHTML = "";
    log.forEach(function (c) {
      var row = document.createElement("div");
      row.className = "log-row";
      var ic = document.createElement("span");
      ic.className = "log-ic";
      ic.innerHTML = '<svg class="ic ic-sm"><use href="#' + (c.type === "video" ? "icon-video" : "icon-phone") + '"/></svg>';
      var meta = document.createElement("div");
      meta.className = "log-meta";
      var nm = document.createElement("p");
      nm.className = "log-name";
      nm.textContent = (c.dir === "out" ? "Outgoing " : "Incoming ") + (c.type === "video" ? "Video Call" : "Voice Call");
      var sub = document.createElement("p");
      sub.className = "log-sub";
      sub.textContent = fmtDur(c.dur) + " · " + fmtAgo(c.at);
      meta.appendChild(nm);
      meta.appendChild(sub);
      row.appendChild(ic);
      row.appendChild(meta);
      list.appendChild(row);
    });
  }

  /* ---------------- saved devices ---------------- */
  function saveDevice(id) {
    if (!id || id === state.myId) return;
    var devs = lsGet("necshare_devices_v1", []);
    devs = devs.filter(function (d) { return d.id !== id; });
    devs.unshift({ id: id, lastSeen: Date.now() });
    lsSet("necshare_devices_v1", devs.slice(0, 10));
    renderDevices();
  }

  function renderDevices() {
    var list = $("deviceList");
    var devs = lsGet("necshare_devices_v1", []);
    // Never list ourselves.
    devs = devs.filter(function (d) { return d.id && d.id !== state.myId; });
    $("deviceEmpty").hidden = devs.length !== 0;
    list.innerHTML = "";
    devs.forEach(function (d) {
      var row = document.createElement("div");
      row.className = "dev-row";
      var ic = document.createElement("span");
      ic.className = "dev-ic";
      ic.innerHTML = '<svg class="ic ic-sm"><use href="#icon-device"/></svg>';
      var meta = document.createElement("div");
      meta.className = "dev-meta";
      var nm = document.createElement("p");
      nm.className = "dev-name";
      nm.textContent = "Device " + shortId(d.id);
      var sub = document.createElement("p");
      sub.className = "dev-sub";
      sub.textContent = "Last paired " + fmtAgo(d.lastSeen);
      meta.appendChild(nm);
      meta.appendChild(sub);
      var go = document.createElement("button");
      go.className = "dev-go";
      go.textContent = "Reconnect";
      go.addEventListener("click", function () { connectToPeer(d.id); });
      row.appendChild(ic);
      row.appendChild(meta);
      row.appendChild(go);
      list.appendChild(row);
    });
  }

  /* ---------------- header status ---------------- */
  function setStatus(text, on) {
    $("statusText").textContent = text;
    $("statusDot").classList.toggle("on", !!on);
  }

  /* ---------------- screens / tabs ---------------- */
  function showScreen(name) {
    ["files", "chat", "calls", "connect"].forEach(function (s) {
      var sec = $("screen-" + s);
      var active = s === name;
      sec.classList.toggle("active", active);
      if (active) sec.removeAttribute("hidden"); else sec.setAttribute("hidden", "");
    });
    [["tabFiles", "files"], ["tabChat", "chat"], ["tabCalls", "calls"], ["tabConnect", "connect"]].forEach(function (p) {
      $(p[0]).classList.toggle("active", p[1] === name);
    });
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  /* ---------------- files tab helpers ---------------- */
  function isConnected() {
    return !!(state.dc && state.dc.readyState === "open");
  }

  function pickFiles() {
    if (!isConnected()) { toast("Connect with a peer first"); return; }
    state.attachForChat = false;
    $("fileInputMulti").click();
  }

  function handlePickedFiles(fileList) {
    if (!fileList || !fileList.length) return;
    if (!isConnected()) { toast("Not connected"); return; }
    for (var i = 0; i < fileList.length; i++) {
      sendFile(fileList[i], { chat: state.attachForChat && i === 0 });
    }
    state.attachForChat = false;
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
          showScreen("connect");
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
    $("tabFiles").addEventListener("click", function () { showScreen("files"); });
    $("tabChat").addEventListener("click", function () { showScreen("chat"); });
    $("tabCalls").addEventListener("click", function () { showScreen("calls"); });
    $("tabConnect").addEventListener("click", function () { showScreen("connect"); });

    // Connect tab
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

    // Files tab
    $("btnSendFiles").addEventListener("click", pickFiles);
    $("btnReceive").addEventListener("click", function () {
      var panel = $("activePanel");
      if (panel.hidden && !isConnected()) { toast("Connect with a peer first"); return; }
      if (panel.hidden) {
        panel.hidden = false;
        toast("Waiting for incoming files");
      }
      panel.scrollIntoView({ behavior: "smooth", block: "center" });
    });
    var dz = $("dropZone");
    dz.addEventListener("click", pickFiles);
    dz.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); pickFiles(); }
    });
    ["dragenter", "dragover"].forEach(function (ev) {
      dz.addEventListener(ev, function (e) { e.preventDefault(); dz.classList.add("over"); });
    });
    ["dragleave", "drop"].forEach(function (ev) {
      dz.addEventListener(ev, function (e) { e.preventDefault(); dz.classList.remove("over"); });
    });
    dz.addEventListener("drop", function (e) {
      var files = e.dataTransfer && e.dataTransfer.files;
      if (files && files.length) handlePickedFiles(files);
    });
    $("fileInputMulti").addEventListener("change", function () {
      var files = $("fileInputMulti").files;
      $("fileInputMulti").value = "";
      handlePickedFiles(files);
    });
    $("btnClearHistory").addEventListener("click", clearHistory);

    // Chat tab
    $("btnSend").addEventListener("click", sendMsg);
    $("chatInput").addEventListener("keydown", function (e) {
      if (e.key === "Enter") sendMsg();
    });
    $("btnAttach").addEventListener("click", function () {
      if (!isConnected()) { toast("Connect with a peer first"); return; }
      state.attachForChat = true;
      $("fileInputMulti").click();
    });
    $("btnVoice").addEventListener("click", toggleVoiceRec);
    $("btnReconnect").addEventListener("click", reconnect);

    // Calls tab
    $("btnCallVoice").addEventListener("click", function () { startCall("audio"); });
    $("btnCallVideo").addEventListener("click", function () { startCall("video"); });
    $("btnVoiceCall").addEventListener("click", function () { startCall("audio"); });
    $("btnVideoCall").addEventListener("click", function () { startCall("video"); });
    $("btnAcceptCall").addEventListener("click", acceptCall);
    $("btnDeclineCall").addEventListener("click", declineCall);
    $("btnHangup").addEventListener("click", hangup);
    $("btnMic").addEventListener("click", toggleMic);
    $("btnCam").addEventListener("click", toggleCam);
    $("btnSpeaker").addEventListener("click", toggleSpeaker);

    // Go-to-connect shortcuts
    ["btnGoConnect0", "btnGoConnect1", "btnGoConnect2"].forEach(function (id) {
      $(id).addEventListener("click", function () { showScreen("connect"); });
    });

    renderHistory();
    renderCallLog();
    renderDevices();
    renderRequests();
    watchInbox();
    watchSignals();
    goOnline();
    publishCode();
    setInterval(tickCodeTimer, 1000);
    handleInviteParam();

    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("sw.js").catch(function () {});
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();
