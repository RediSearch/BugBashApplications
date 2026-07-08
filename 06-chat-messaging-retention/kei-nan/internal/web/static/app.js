"use strict";

const C = {
  blue: "#4c8dff", green: "#35c28f", amber: "#f2b134", red: "#ef5f6b",
  purple: "#a97bff", line: "#262b36", muted: "#9aa4b2", text: "#e6e8eb",
};
const SEARCH_PROFILES = ["channel_search", "thread_search", "tag_filter", "text_prefix", "deep_pagination"];

let selectedChannel = null;
const histRate = []; // {t, ingest, expire}
const histLat = [];  // {t, p50, p99}
const HIST_MAX = 600;

async function getJSON(url) { const r = await fetch(url); if (!r.ok) throw new Error(r.status + " " + url); return r.json(); }
async function postJSON(url, body) {
  const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  if (!r.ok) throw new Error(r.status + " " + url);
  return r.json();
}

const fmtNum = (n) => {
  if (n == null) return "—";
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(Math.round(n));
};
const fmtBytes = (n) => {
  if (!n) return "0 B"; const u = ["B", "KB", "MB", "GB", "TB"]; let i = 0, f = n;
  while (f >= 1024 && i < u.length - 1) { f /= 1024; i++; } return f.toFixed(1) + " " + u[i];
};
const escapeHtml = (s) => (s || "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

// ---------- high-level KPIs + badges + index status ----------

function kpi(k, v, u, cls) {
  return `<div class="kpi ${cls || ""}"><div class="k">${k}</div><div class="v">${v}<span class="u"> ${u || ""}</span></div></div>`;
}

function renderStats(s) {
  const idx = s.index || {};
  const foot = fmtBytes(s.footprint.bytes);
  const staleCls = s.correctness.stale_hits > 0 ? "hot" : "good";
  document.getElementById("kpis").innerHTML = [
    kpi("messages retained", fmtNum(idx.num_docs), ""),
    kpi("ingesting", fmtNum(s.rates.ingest), "/s"),
    kpi("expiring", fmtNum(s.rates.expire), "/s"),
    kpi(s.disk_mode ? "on-disk footprint" : "used memory", foot, "", s.footprint.plateau ? "good" : ""),
    kpi("stale results", fmtNum(s.correctness.stale_hits), "", staleCls),
    kpi("query p99", s.latency.p99.toFixed(0), "ms"),
    kpi("slow queries", fmtNum(s.latency.slow), "≥" + s.correctness.slow_thresh_ms + "ms"),
    kpi("recall", s.correctness.recall_pct.toFixed(0), "%"),
  ].join("");

  document.getElementById("badge-elapsed").textContent = "t=" + Math.round(s.elapsed_sec) + "s";
  const mode = document.getElementById("badge-mode");
  mode.textContent = s.disk_mode ? "disk-backed index" : "in-RAM index"; mode.className = "badge " + (s.disk_mode ? "ok" : "warn");
  const cor = document.getElementById("badge-correct");
  if (s.correctness.stale_hits > 0) { cor.textContent = "STALE HITS: " + s.correctness.stale_hits; cor.className = "badge bad"; }
  else { cor.textContent = "no stale hits ✓"; cor.className = "badge ok"; }
  const pl = document.getElementById("badge-plateau");
  if (s.footprint.samples < 6) { pl.textContent = "footprint: warming up"; pl.className = "badge ghost"; }
  else if (s.footprint.plateau) { pl.textContent = "footprint: PLATEAU"; pl.className = "badge ok"; }
  else { pl.textContent = "footprint: GROWING"; pl.className = "badge warn"; }
  document.getElementById("verdict-note").textContent = s.footprint.note || "";

  histRate.push({ t: s.elapsed_sec, ingest: s.rates.ingest, expire: s.rates.expire });
  histLat.push({ t: s.elapsed_sec, p50: s.latency.p50, p99: s.latency.p99 });
  if (histRate.length > HIST_MAX) histRate.shift();
  if (histLat.length > HIST_MAX) histLat.shift();

  renderIndexStatus(idx, s.disk_mode);
  drawCharts(s);
}

function srow(k, v, cls) { return `<div class="srow"><span class="sk">${k}</span><span class="sv ${cls || ""}">${v}</span></div>`; }

function renderIndexStatus(idx, disk) {
  const failCls = idx.hash_indexing_failures > 0 ? "" : "";
  let html = `<div class="sec">index · FT.INFO</div>`;
  html += srow("documents", fmtNum(idx.num_docs));
  html += srow("index records", fmtNum(idx.num_records));
  html += srow("max doc id", fmtNum(idx.max_doc_id));
  html += srow("inverted size (est)", idx.inverted_sz_mb.toFixed(2) + " MB");
  html += srow("doc-table (est)", idx.doc_table_size_mb.toFixed(2) + " MB");
  html += srow("total index mem (est)", idx.total_index_memory_sz_mb.toFixed(2) + " MB");
  html += srow("indexing", idx.indexing ? "yes" : "no");
  html += srow("percent indexed", (idx.percent_indexed * 100).toFixed(1) + " %");
  html += srow("GC / cleaning", idx.cleaning ? "running" : "idle");
  html += `<div class="srow"><span class="sk">hash indexing failures</span><span class="sv" style="${idx.hash_indexing_failures > 0 ? "color:var(--red)" : ""}">${fmtNum(idx.hash_indexing_failures)}</span></div>`;
  html += `<div class="sec">storage · INFO</div>`;
  if (disk) {
    html += srow("on-disk usage", fmtBytes(idx.disk_usage));
    html += srow("expired reads served", fmtNum(idx.async_reads_expired));
    html += srow("compaction cycles", fmtNum(idx.compaction_cycles));
    html += srow("pending compaction", fmtBytes(idx.pending_compaction));
  } else {
    html += srow("process memory", fmtBytes(idx.used_mem));
    html += `<div class="srow"><span class="sk" style="color:var(--amber)">in-RAM (Redis Stack)</span><span class="sv muted">disk metrics N/A</span></div>`;
  }
  document.getElementById("index-status").innerHTML = html;
}

// ---------- charts ----------

function prepCanvas(cv) {
  const dpr = window.devicePixelRatio || 1, w = cv.clientWidth, h = cv.clientHeight || 170;
  cv.width = w * dpr; cv.height = h * dpr; const ctx = cv.getContext("2d"); ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w, h };
}
function drawLine(cv, series, yFmt) {
  const { ctx, w, h } = prepCanvas(cv); ctx.clearRect(0, 0, w, h);
  const padL = 54, padR = 10, padT = 12, padB = 20;
  const all = series.flatMap((s) => s.pts);
  if (all.length < 2) { ctx.fillStyle = C.muted; ctx.font = "12px sans-serif"; ctx.fillText("collecting…", padL, h / 2); return; }
  let xmin = Infinity, xmax = -Infinity, ymax = -Infinity;
  for (const p of all) { xmin = Math.min(xmin, p[0]); xmax = Math.max(xmax, p[0]); ymax = Math.max(ymax, p[1]); }
  if (xmax === xmin) xmax = xmin + 1; if (ymax <= 0) ymax = 1; ymax *= 1.12;
  const X = (x) => padL + ((x - xmin) / (xmax - xmin)) * (w - padL - padR);
  const Y = (y) => h - padB - (y / ymax) * (h - padT - padB);
  ctx.strokeStyle = C.line; ctx.fillStyle = C.muted; ctx.font = "10px sans-serif"; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) { const yv = (ymax / 4) * i, y = Y(yv); ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke(); ctx.fillText(yFmt(yv), 4, y + 3); }
  ctx.fillText(Math.round(xmin) + "s", padL, h - 5); ctx.fillText(Math.round(xmax) + "s", w - padR - 24, h - 5);
  for (const s of series) { ctx.strokeStyle = s.color; ctx.lineWidth = 2; ctx.beginPath(); s.pts.forEach((p, i) => { const x = X(p[0]), y = Y(p[1]); i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); }); ctx.stroke(); }
  let lx = padL; ctx.font = "11px sans-serif";
  for (const s of series) { ctx.fillStyle = s.color; ctx.fillRect(lx, padT - 6, 10, 10); ctx.fillStyle = C.text; ctx.fillText(s.name, lx + 14, padT + 3); lx += 24 + ctx.measureText(s.name).width; }
}
function drawCharts(s) {
  const series = s.series || [], useDisk = s.disk_mode;
  document.getElementById("cap-foot").textContent = useDisk ? "on-disk footprint (search_disk_usage)" : "process memory (used_memory)";
  drawLine(document.getElementById("c-foot"), [{ name: useDisk ? "disk" : "mem", color: C.blue, pts: series.map((p) => [p.t, useDisk ? p.disk_usage : p.used_mem]) }], fmtBytes);
  drawLine(document.getElementById("c-docs"), [
    { name: "num_docs", color: C.green, pts: series.map((p) => [p.t, p.num_docs]) },
    { name: "num_records", color: C.amber, pts: series.map((p) => [p.t, p.num_records]) }], fmtNum);
  drawLine(document.getElementById("c-rate2"), [
    { name: "ingest/s", color: C.green, pts: histRate.map((p) => [p.t, p.ingest]) },
    { name: "expire/s", color: C.red, pts: histRate.map((p) => [p.t, p.expire]) }], fmtNum);
  drawLine(document.getElementById("c-lat"), [
    { name: "p50", color: C.amber, pts: histLat.map((p) => [p.t, p.p50]) },
    { name: "p99", color: C.red, pts: histLat.map((p) => [p.t, p.p99]) }], (v) => v.toFixed(0));
}

// ---------- query controls ----------

async function loadControl() {
  try {
    const c = await getJSON("/api/control");
    document.getElementById("c-rate").value = c.rate;
    document.getElementById("c-timeout").value = c.timeout_ms;
    document.getElementById("c-limit").value = c.limit;
    document.getElementById("mix").innerHTML = c.profiles.map((p) =>
      `<div class="mixrow"><div class="nm">${p.name}<small>${escapeHtml(p.desc)} · ${fmtNum(p.count)} run</small></div>
        <input data-prof="${p.name}" type="number" min="0" value="${p.weight}" /></div>`).join("");
  } catch (e) { /* ignore */ }
}
async function applyControl() {
  const body = {
    rate: parseInt(document.getElementById("c-rate").value, 10) || 0,
    timeout_ms: parseInt(document.getElementById("c-timeout").value, 10) || 0,
    limit: parseInt(document.getElementById("c-limit").value, 10) || 20,
  };
  try { await postJSON("/api/control", body); loadControl(); } catch (e) {}
}
async function applyMix() {
  const profiles = [...document.querySelectorAll("#mix input[data-prof]")].map((i) => ({ name: i.dataset.prof, weight: parseInt(i.value, 10) || 0 }));
  if (!profiles.some((p) => p.weight > 0)) { alert("at least one profile weight must be > 0"); return; }
  try { await postJSON("/api/control", { profiles }); loadControl(); } catch (e) {}
}

// ---------- ad-hoc queries ----------

function renderProfileChecks() {
  document.getElementById("q-profiles").innerHTML = SEARCH_PROFILES.map((p) =>
    `<label><input type="checkbox" class="qp" value="${p}" checked /> ${p}</label>`).join("");
}
async function genQueries() {
  const count = parseInt(document.getElementById("q-count").value, 10) || 8;
  const profiles = [...document.querySelectorAll(".qp:checked")].map((c) => c.value);
  try {
    const qs = await postJSON("/api/gen-queries", { count, profiles });
    document.getElementById("q-text").value = qs.map((q) => q.query).join("\n");
  } catch (e) {}
}
async function sendQueries() {
  const queries = document.getElementById("q-text").value.split("\n").map((l) => l.trim()).filter(Boolean);
  if (!queries.length) return;
  const res = document.getElementById("q-results");
  res.innerHTML = `<div class="muted">running ${queries.length}…</div>`;
  try {
    const out = await postJSON("/api/run-queries", { queries });
    res.innerHTML = out.map((r) => {
      const cls = r.error ? "rres err" : "rres";
      const t = r.error ? "ERR" : "total " + fmtNum(r.total);
      const detail = r.error ? escapeHtml(r.error) : `${r.ms.toFixed(1)}ms · ${r.returned} keys`;
      return `<div class="${cls}"><span class="rq">${escapeHtml(r.query)}</span><span class="rt">${t}</span><span class="rm">${detail}</span></div>`;
    }).join("");
  } catch (e) { res.innerHTML = `<div class="muted">request failed</div>`; }
}

// ---------- conversations ----------

async function refreshChannels() {
  try {
    const chans = await getJSON("/api/channels");
    const el = document.getElementById("channels");
    if (!chans.length) { el.innerHTML = '<div class="muted">no live channels yet…</div>'; return; }
    if (!selectedChannel) selectedChannel = chans[0].channel;
    el.innerHTML = chans.map((c) => `<div class="chan ${c.channel === selectedChannel ? "active" : ""}" data-ch="${c.channel}"><span>${c.channel}</span><span class="cnt">${fmtNum(c.count)}</span></div>`).join("");
    el.querySelectorAll(".chan").forEach((n) => n.addEventListener("click", () => { selectedChannel = n.dataset.ch; refreshChannels(); refreshMessages(); }));
  } catch (e) {}
}
function msgCard(m) {
  const secs = m.ttl_ms > 0 ? Math.round(m.ttl_ms / 1000) : -1;
  const plan = (m.plan || "").toLowerCase();
  return `<div class="msg" data-ttl="${m.ttl_ms}" data-fetched="${Date.now()}">
    <div class="meta"><span class="pill ${plan}">${m.plan || "?"}</span><span>${m.channel}</span><span>·</span><span>${m.user}</span><span>·</span><span>${m.thread}</span>
      <span class="ttl">⏳ <b class="ttlv">${secs < 0 ? "no ttl" : secs + "s"}</b></span></div>
    <div class="body">${escapeHtml(m.body)}</div><div class="ttlbar"><i style="width:100%"></i></div></div>`;
}
async function refreshMessages() {
  if (!selectedChannel) return;
  try {
    const r = await getJSON("/api/messages?channel=" + encodeURIComponent(selectedChannel) + "&limit=30");
    const el = document.getElementById("messages");
    el.innerHTML = (r.messages && r.messages.length) ? r.messages.map(msgCard).join("") : `<div class="muted">no messages in ${selectedChannel} (total match ${r.total})</div>`;
  } catch (e) {}
}
function tickTTLs() {
  document.querySelectorAll(".msg").forEach((n) => {
    const ttl0 = parseInt(n.dataset.ttl, 10); if (isNaN(ttl0) || ttl0 < 0) return;
    const rem = ttl0 - (Date.now() - parseInt(n.dataset.fetched, 10));
    const v = n.querySelector(".ttlv"), bar = n.querySelector(".ttlbar > i");
    if (rem <= 0) { if (v) v.textContent = "expiring…"; if (bar) { bar.style.width = "0%"; bar.style.background = C.red; } n.style.opacity = "0.5"; return; }
    if (v) v.textContent = Math.ceil(rem / 1000) + "s"; if (bar) bar.style.width = Math.max(0, Math.min(100, (rem / ttl0) * 100)) + "%";
  });
}

// ---------- scoped search ----------

async function runSearch() {
  const p = new URLSearchParams();
  const ch = s_val("s-channel"), us = s_val("s-user"), th = s_val("s-thread"), tx = s_val("s-text");
  if (ch) p.set("channel", ch); if (us) p.set("user", us); if (th) p.set("thread", th); if (tx) p.set("q", tx);
  p.set("limit", "30");
  try {
    const r = await getJSON("/api/search?" + p.toString());
    document.getElementById("s-query").textContent = "FT.SEARCH " + JSON.stringify(r.query) + "  →  total " + r.total;
    const el = document.getElementById("s-results");
    if (r.error) { el.innerHTML = `<div class="muted">error: ${escapeHtml(r.error)}</div>`; return; }
    el.innerHTML = (r.messages && r.messages.length) ? r.messages.map(msgCard).join("") : `<div class="muted">no matches</div>`;
  } catch (e) { document.getElementById("s-results").innerHTML = `<div class="muted">request failed</div>`; }
}
const s_val = (id) => document.getElementById(id).value.trim();

// ---------- config + boot ----------

async function loadConfig() {
  try {
    const c = await getJSON("/api/config");
    document.getElementById("cfg-sub").textContent =
      `index "${c.index}" @ ${c.addr} · ${fmtNum(c.channels)} channels · ${fmtNum(c.users)} users` + (c.vocab_high ? ` · vocab+${fmtNum(c.vocab_high)}` : "");
    document.getElementById("tiers").textContent = "retention tiers: " + c.tiers.map((t) => `${t.name} (${t.ttl}, w${t.weight})`).join("  ·  ");
  } catch (e) {}
}
async function tickStats() { try { renderStats(await getJSON("/api/stats")); } catch (e) {} }

loadConfig(); loadControl(); renderProfileChecks(); tickStats(); refreshChannels(); refreshMessages();
document.getElementById("c-apply").addEventListener("click", applyControl);
document.getElementById("c-apply-mix").addEventListener("click", applyMix);
document.getElementById("q-gen").addEventListener("click", genQueries);
document.getElementById("q-send").addEventListener("click", sendQueries);
document.getElementById("s-run").addEventListener("click", runSearch);
document.getElementById("s-text").addEventListener("keydown", (e) => { if (e.key === "Enter") runSearch(); });

setInterval(tickStats, 2000);
setInterval(refreshChannels, 6000);
setInterval(refreshMessages, 3000);
setInterval(tickTTLs, 1000);
setInterval(loadControl, 5000);
