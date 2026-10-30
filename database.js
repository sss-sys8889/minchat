"use strict";
const { Pool } = require("pg");

let pool = null;
let ready = false;

if (process.env.DATABASE_URL) {
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
    ssl: process.env.PGSSLMODE === "require" ? { rejectUnauthorized: false } : undefined
  });
  pool.on("error", err => console.error("[database] idle client error:", err.message));
}

async function initialize() {
  if (!pool) return false;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS moderation_reports (
      id BIGSERIAL PRIMARY KEY,
      reporter_session VARCHAR(128) NOT NULL,
      reported_session VARCHAR(128),
      reason VARCHAR(200) NOT NULL DEFAULT 'unspecified',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS moderation_reports_created_at_idx
      ON moderation_reports (created_at DESC);
    CREATE TABLE IF NOT EXISTS admin_actions (
      id BIGSERIAL PRIMARY KEY,
      action VARCHAR(80) NOT NULL,
      target_session VARCHAR(128),
      details JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS admin_actions_created_at_idx
      ON admin_actions (created_at DESC);
  `);
  await pool.query("SELECT 1");
  ready = true;
  console.log("[database] PostgreSQL connected and tables are ready");
  return true;
}

async function recordReport(reporter, reported, reason) {
  if (!pool || !ready) return false;
  await pool.query(
    "INSERT INTO moderation_reports (reporter_session, reported_session, reason) VALUES ($1, $2, $3)",
    [String(reporter).slice(0, 128), reported ? String(reported).slice(0, 128) : null, String(reason || "unspecified").slice(0, 200)]
  );
  return true;
}

async function recordAdminAction(action, target, details = {}) {
  if (!pool || !ready) return false;
  await pool.query(
    "INSERT INTO admin_actions (action, target_session, details) VALUES ($1, $2, $3::jsonb)",
    [String(action).slice(0, 80), target ? String(target).slice(0, 128) : null, JSON.stringify(details)]
  );
  return true;
}

async function getRecentLogs(limit = 50) {
  if (!pool || !ready) return { reports: [], actions: [] };
  const [reports, actions] = await Promise.all([
    pool.query("SELECT id, reporter_session, reported_session, reason, created_at FROM moderation_reports ORDER BY created_at DESC LIMIT $1", [limit]),
    pool.query("SELECT id, action, target_session, details, created_at FROM admin_actions ORDER BY created_at DESC LIMIT $1", [limit])
  ]);
  return { reports: reports.rows, actions: actions.rows };
}

async function status() {
  if (!pool) return { configured: false, connected: false };
  try {
    await pool.query("SELECT 1");
    ready = true;
    return { configured: true, connected: true };
  } catch {
    ready = false;
    return { configured: true, connected: false };
  }
}

module.exports = { initialize, recordReport, recordAdminAction, getRecentLogs, status };
