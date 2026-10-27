"use strict";
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: false },
  maxHttpBufferSize: 1e6,
  pingInterval: 25000,
  pingTimeout: 20000
});

app.disable("x-powered-by");
app.use(express.json({ limit: "20kb" }));
app.use(express.static("public", { etag: true, maxAge: "1h" }));
app.get("/health", (_req, res) => res.json({ ok: true, app: "Minchat", version: "1.0.0" }));
// Optional TURN support: configure TURN_URL, TURN_USERNAME and TURN_CREDENTIAL on the host.
// Credentials are delivered only to connected clients; use short-lived TURN credentials in production.
app.get("/ice-config", (_req, res) => {
  const servers = [{ urls: ["stun:stun.l.google.com:19302"] }];
  if (process.env.TURN_URL && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL) {
    servers.push({
      urls: process.env.TURN_URL.split(",").map(x => x.trim()).filter(Boolean),
      username: process.env.TURN_USERNAME,
      credential: process.env.TURN_CREDENTIAL
    });
  }
  res.set("Cache-Control", "no-store");
  res.json({ iceServers: servers });
});

// In-memory queue only. No video/audio is sent through this server or saved.
const waiting = new Map(); // socketId -> safe matchmaking preferences
const partner = new Map(); // socketId -> socketId
const reports = new Map(); // per-process rate limit only; report details are not persisted
const sessions = new Map(); // online session metadata only; no persistent user accounts
const matchHistory = new Map(); // recent peer socket IDs, in memory only
const previousInvites = new Map(); // target socket ID -> requester socket ID

function adminAuthorized(req) {
  const configured = process.env.ADMIN_TOKEN;
  const supplied = req.get("x-admin-token") || "";
  const a = Buffer.from(supplied);
  const b = Buffer.from(configured || "");
  return Boolean(configured && supplied && a.length === b.length &&
    require("crypto").timingSafeEqual(a, b));
}
function adminOnly(req, res, next) {
  if (!process.env.ADMIN_TOKEN) return res.status(503).json({ error: "Admin panel is disabled: set ADMIN_TOKEN on the server." });
  if (!adminAuthorized(req)) return res.status(401).json({ error: "رمز مدیر نامعتبر است." });
  next();
}
app.get("/admin/api/users", adminOnly, (_req, res) => {
  res.set("Cache-Control", "no-store");
  res.json({ users: [...sessions.entries()].map(([id, u]) => ({ id, ...u, waiting: waiting.has(id), inCall: partner.has(id) })) });
});
app.post("/admin/api/users/:id/remove", adminOnly, (req, res) => {
  const id = req.params.id;
  const target = io.sockets.sockets.get(id);
  if (!target) {
    sessions.delete(id);
    waiting.delete(id);
    return res.status(404).json({ error: "این کاربر دیگر آنلاین نیست." });
  }
  endPair(id);
  sessions.delete(id);
  target.emit("admin-removed", { message: "مدیر دسترسی این نشست را قطع کرد." });
  target.disconnect(true);
  console.log(`[admin] removed online session ${id}`);
  res.json({ ok: true, message: "نشست کاربر از Minchat خارج شد." });
});

function cleanChoice(value, allowed, fallback) {
  return allowed.includes(value) ? value : fallback;
}
const countries = ["ANY", "IR", "TR", "RU", "US", "GB", "DE", "FR", "IN", "BR", "OTHER"];
const genders = ["woman", "man", "other", "unspecified"];
const targets = ["ANY", "woman", "man", "other"];

function compatible(a, b) {
  const aCountry = a.targetCountry === "ANY" || a.targetCountry === b.country;
  const bCountry = b.targetCountry === "ANY" || b.targetCountry === a.country;
  const aGender = a.targetGender === "ANY" || a.targetGender === b.gender;
  const bGender = b.targetGender === "ANY" || b.targetGender === a.gender;
  return aCountry && bCountry && aGender && bGender;
}
function pair(socket, prefs) {
  for (const [otherId, otherPrefs] of waiting.entries()) {
    if (otherId === socket.id) continue;
    const other = io.sockets.sockets.get(otherId);
    if (other && compatible(prefs, otherPrefs)) {
      waiting.delete(otherId);
      waiting.delete(socket.id);
      partner.set(socket.id, otherId);
      partner.set(otherId, socket.id);
      rememberMatch(socket.id, otherId);
      rememberMatch(otherId, socket.id);
      socket.emit("matched", { initiator: true, peerId: otherId, partnerCountry: otherPrefs.country });
      other.emit("matched", { initiator: false, peerId: socket.id, partnerCountry: prefs.country });
      return true;
    }
  }
  waiting.set(socket.id, prefs);
  socket.emit("waiting");
  return false;
}
function rememberMatch(id, peerId) {
  const list = matchHistory.get(id) || [];
  matchHistory.set(id, [peerId, ...list.filter(x => x !== peerId)].slice(0, 10));
}
function endPair(id, notify = true) {
  waiting.delete(id);
  const otherId = partner.get(id);
  partner.delete(id);
  if (otherId) {
    partner.delete(otherId);
    if (notify) io.to(otherId).emit("partner-left");
  }
}
io.on("connection", (socket) => {
  sessions.set(socket.id, { connectedAt: new Date().toISOString(), country: "—", gender: "—" });
  socket.emit("ready", { app: "Minchat", version: "1.0.0" });

  socket.on("find-partner", (input = {}) => {
    endPair(socket.id);
    const prefs = {
      country: cleanChoice(input.country, countries.filter(x => x !== "ANY"), "OTHER"),
      gender: cleanChoice(input.gender, genders, "unspecified"),
      targetCountry: cleanChoice(input.targetCountry, countries, "ANY"),
      targetGender: cleanChoice(input.targetGender, targets, "ANY")
    };
    sessions.set(socket.id, { ...(sessions.get(socket.id) || {}), country: prefs.country, gender: prefs.gender });
    pair(socket, prefs);
  });

  socket.on("signal", ({ to, data } = {}) => {
    if (!to || partner.get(socket.id) !== to || !data || typeof data !== "object") return;
    // WebRTC offer/answer/ICE only; reject unusually large payloads.
    let size;
    try { size = JSON.stringify(data).length; } catch { return; }
    if (size > 200000) return;
    io.to(to).emit("signal", { from: socket.id, data });
  });

  socket.on("skip", () => {
    endPair(socket.id);
    socket.emit("skipped");
  });

  socket.on("previous-request", ({ targetId } = {}) => {
    const history = matchHistory.get(socket.id) || [];
    const target = targetId && io.sockets.sockets.get(targetId);
    if (!history.includes(targetId) || !target || targetId === socket.id || partner.has(socket.id) || partner.has(targetId) || waiting.has(targetId)) {
      socket.emit("previous-unavailable");
      return;
    }
    previousInvites.set(targetId, socket.id);
    target.emit("previous-invite", { from: socket.id });
    socket.emit("previous-pending");
  });

  socket.on("previous-response", ({ from, accept } = {}) => {
    if (previousInvites.get(socket.id) !== from) return;
    previousInvites.delete(socket.id);
    const requester = io.sockets.sockets.get(from);
    if (!accept || !requester || partner.has(socket.id) || partner.has(from)) {
      if (requester) requester.emit("previous-unavailable");
      return;
    }
    waiting.delete(socket.id);
    waiting.delete(from);
    partner.set(from, socket.id);
    partner.set(socket.id, from);
    rememberMatch(from, socket.id);
    rememberMatch(socket.id, from);
    requester.emit("matched", { initiator: true, peerId: socket.id, partnerCountry: sessions.get(socket.id)?.country || "—" });
    socket.emit("matched", { initiator: false, peerId: from, partnerCountry: sessions.get(from)?.country || "—" });
  });

  socket.on("report", ({ reason } = {}) => {
    const now = Date.now();
    const last = reports.get(socket.id) || 0;
    if (now - last < 10000) return;
    reports.set(socket.id, now);
    const otherId = partner.get(socket.id);
    // MVP behavior: disconnect the pair and log only a generic event, not media or chat.
    console.log(`[moderation] report received; reporter=${socket.id}; reason=${String(reason || "unspecified").slice(0, 40)}`);
    endPair(socket.id);
    socket.emit("report-received");
    if (otherId) io.to(otherId).emit("partner-left");
  });

  socket.on("disconnect", () => {
    endPair(socket.id);
    reports.delete(socket.id);
    sessions.delete(socket.id);
    matchHistory.delete(socket.id);
    previousInvites.delete(socket.id);
    for (const [targetId, requesterId] of previousInvites.entries()) {
      if (requesterId === socket.id) previousInvites.delete(targetId);
    }
  });
});

const port = Number(process.env.PORT || 3000);
server.listen(port, "0.0.0.0", () => console.log(`Minchat listening on port ${port}`));
