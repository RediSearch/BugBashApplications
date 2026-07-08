"use strict";

const C = {
  blue: "#4c8dff", green: "#35c28f", amber: "#f2b134", red: "#ef5f6b",
  purple: "#a97bff", line: "#262b36", muted: "#9aa4b2", text: "#e6e8eb",
};

let selectedChannel = null;
const histRate = []; // {t, ingest, expire}
const histLat = [];  // {t, p50, p99}
const HIST_MAX = 600;

async function getJSON(url) { const r = await fetch(url); if (!r.ok) throw new Error(r.status + " " + url); return r.json(); }
async function postJSON(url, body) {
  const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  if (!r.ok) throw new Error(r.status + " " + url); return r.json();
}
const fmtNum = (n) => {
  if (n == null) return "—";
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(Math.round(n));
};
const fmtBytes = (n) => { if (!n) return "0 B"; const u = ["B", "KB", "MB", "GB", "TB"]; let i = 0, f = n; while (f >= 1024 && i < u.length - 1) { f /= 1024; i++; } return f.toFixed(1) + " " + u[i]; };
const escapeHtml = (s) => (s || "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

// ---------- collapsible panels ----------
document.querySelectorAll(".panel-head[data-collapse]").forEach((h) =>
  h.addEventListener("click", () => h.closest(".panel").classList.toggle("collapsed")));

// ---------- KPIs + badges + index status ----------

function kpi(k, v, u, cls, help) {
  return `<div class="kpi ${cls || ""}" title="${escapeHtml(help || "")}"><div class="k">${k}</div><div class="v">${v}<span class="u"> ${u || ""}</span></div></div>`;
}

const HELP = {
  retained: "Live documents currently in the index (FT.INFO num_docs). Under TTL churn this should plateau, not grow without bound.",
  ingest: "Messages written per second (HSET + whole-key PEXPIRE).",
  expire: "Estimated messages expiring per second ≈ ingest − doc-growth − deletes. At steady state this converges on the ingest rate.",
  footprint: "On-disk bytes (search_disk_usage) — or process memory when in-RAM. PLATEAU = the disk footprint is stable under expiry churn; GROWING = watch for unbounded growth.",
  stale: "Correctness signal. A stale hit is a query result that was ALREADY EXPIRED (or deleted) before the query ran — it must never be returned. 0 = the index correctly hides expired docs at query time; >0 = it is leaking expired content (a bug). OSS/in-RAM RediSearch shows many under churn; the disk build should stay 0.",
  p99: "99th-percentile query latency across all query profiles.",
  slow: "Queries at/over the slow threshold (default = the 500ms server timeout) — a timeout-risk signal, driven by heavy profiles (deep pagination, wide GROUPBY).",
  recall: "Of channel-scoped queries for a known-live message, the % that actually returned it. Misses are usually async-indexing lag.",
};

function renderStats(s) {
  const idx = s.index || {};
  document.getElementById("kpis").innerHTML = [
    kpi("messages retained", fmtNum(idx.num_docs), "", "", HELP.retained),
    kpi("ingesting", fmtNum(s.rates.ingest), "/s", "", HELP.ingest),
    kpi("expiring", fmtNum(s.rates.expire), "/s", "", HELP.expire),
    kpi(s.disk_mode ? "on-disk footprint" : "used memory", fmtBytes(s.footprint.bytes), "", s.footprint.plateau ? "good" : "", HELP.footprint),
    kpi("stale results", fmtNum(s.correctness.stale_hits), "", s.correctness.stale_hits > 0 ? "hot" : "good", HELP.stale),
    kpi("query p99", s.latency.p99.toFixed(0), "ms", "", HELP.p99),
    kpi("slow queries", fmtNum(s.latency.slow), "≥" + s.correctness.slow_thresh_ms + "ms", "", HELP.slow),
    kpi("recall", s.correctness.recall_pct.toFixed(0), "%", "", HELP.recall),
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

const srow = (k, v, style) => `<div class="srow"><span class="sk">${k}</span><span class="sv" style="${style || ""}">${v}</span></div>`;
function renderIndexStatus(idx, disk) {
  let h = `<div class="sec">index · FT.INFO</div>`;
  h += srow("documents", fmtNum(idx.num_docs));
  h += srow("index records", fmtNum(idx.num_records));
  h += srow("max doc id", fmtNum(idx.max_doc_id));
  h += srow("inverted size (est)", (idx.inverted_sz_mb || 0).toFixed(2) + " MB");
  h += srow("doc-table (est)", (idx.doc_table_size_mb || 0).toFixed(2) + " MB");
  h += srow("total index mem (est)", (idx.total_index_memory_sz_mb || 0).toFixed(2) + " MB");
  h += srow("indexing", idx.indexing ? "yes" : "no");
  h += srow("percent indexed", ((idx.percent_indexed || 0) * 100).toFixed(1) + " %");
  h += srow("GC / cleaning", idx.cleaning ? "running" : "idle");
  h += srow("hash indexing failures", fmtNum(idx.hash_indexing_failures), idx.hash_indexing_failures > 0 ? "color:var(--red)" : "");
  h += `<div class="sec">storage · INFO</div>`;
  if (disk) {
    h += srow("on-disk usage", fmtBytes(idx.disk_usage));
    h += srow("expired reads served", fmtNum(idx.async_reads_expired));
    h += srow("compaction cycles", fmtNum(idx.compaction_cycles));
    h += srow("pending compaction", fmtBytes(idx.pending_compaction));
  } else {
    h += srow("process memory", fmtBytes(idx.used_mem));
    h += srow("in-RAM (Redis Stack)", "disk metrics N/A", "color:var(--amber)");
  }
  document.getElementById("index-status").innerHTML = h;
}

// ---------- charts (with hover tooltips) ----------

const tip = document.getElementById("tooltip");

function prepCanvas(cv) {
  const dpr = window.devicePixelRatio || 1, w = cv.clientWidth, h = cv.clientHeight || 170;
  cv.width = w * dpr; cv.height = h * dpr; const ctx = cv.getContext("2d"); ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w, h };
}
function computeChart(cv, series, yFmt) {
  const { w, h } = prepCanvas(cv);
  const padL = 54, padR = 10, padT = 12, padB = 20;
  const all = series.flatMap((s) => s.pts);
  let xmin = Infinity, xmax = -Infinity, ymax = -Infinity;
  for (const p of all) { xmin = Math.min(xmin, p[0]); xmax = Math.max(xmax, p[0]); ymax = Math.max(ymax, p[1]); }
  if (!isFinite(xmin)) { xmin = 0; xmax = 1; }
  if (xmax === xmin) xmax = xmin + 1; if (!(ymax > 0)) ymax = 1; ymax *= 1.12;
  return { series, yFmt, xmin, xmax, ymax, padL, padR, padT, padB, w, h };
}
function XY(c) {
  return {
    X: (x) => c.padL + ((x - c.xmin) / (c.xmax - c.xmin)) * (c.w - c.padL - c.padR),
    Y: (y) => c.h - c.padB - (y / c.ymax) * (c.h - c.padT - c.padB),
  };
}
function renderChart(cv, c) {
  const { ctx } = prepCanvas(cv); const { X, Y } = XY(c);
  ctx.clearRect(0, 0, c.w, c.h);
  const all = c.series.flatMap((s) => s.pts);
  if (all.length < 2) { ctx.fillStyle = C.muted; ctx.font = "12px sans-serif"; ctx.fillText("collecting…", c.padL, c.h / 2); return; }
  ctx.strokeStyle = C.line; ctx.fillStyle = C.muted; ctx.font = "10px sans-serif"; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) { const yv = (c.ymax / 4) * i, y = Y(yv); ctx.beginPath(); ctx.moveTo(c.padL, y); ctx.lineTo(c.w - c.padR, y); ctx.stroke(); ctx.fillText(c.yFmt(yv), 4, y + 3); }
  ctx.fillText(Math.round(c.xmin) + "s", c.padL, c.h - 5); ctx.fillText(Math.round(c.xmax) + "s", c.w - c.padR - 24, c.h - 5);
  for (const s of c.series) { ctx.strokeStyle = s.color; ctx.lineWidth = 2; ctx.beginPath(); s.pts.forEach((p, i) => { const x = X(p[0]), y = Y(p[1]); i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); }); ctx.stroke(); }
  let lx = c.padL; ctx.font = "11px sans-serif";
  for (const s of c.series) { ctx.fillStyle = s.color; ctx.fillRect(lx, c.padT - 6, 10, 10); ctx.fillStyle = C.text; ctx.fillText(s.name, lx + 14, c.padT + 3); lx += 24 + ctx.measureText(s.name).width; }
}
function drawLine(cv, series, yFmt) {
  if (cv.clientWidth < 60) return; // panel collapsed / hidden
  const c = computeChart(cv, series, yFmt); cv._chart = c; renderChart(cv, c);
  if (!cv._wired) {
    cv._wired = true;
    cv.addEventListener("mousemove", (e) => hoverChart(cv, e));
    cv.addEventListener("mouseleave", () => { tip.style.display = "none"; if (cv._chart) renderChart(cv, cv._chart); });
  }
}
function nearest(pts, dx) { let best = null, bd = Infinity; for (const p of pts) { const d = Math.abs(p[0] - dx); if (d < bd) { bd = d; best = p; } } return best; }
function hoverChart(cv, e) {
  const c = cv._chart; if (!c || !c.series.length) return;
  const rect = cv.getBoundingClientRect();
  const mx = (e.clientX - rect.left) * (c.w / rect.width);
  if (mx < c.padL || mx > c.w - c.padR) { tip.style.display = "none"; renderChart(cv, c); return; }
  const dx = c.xmin + ((mx - c.padL) / (c.w - c.padL - c.padR)) * (c.xmax - c.xmin);
  const ref = c.series.reduce((a, b) => (b.pts.length > a.pts.length ? b : a), c.series[0]);
  const rp = nearest(ref.pts, dx); if (!rp) return;
  const tx = rp[0];
  renderChart(cv, c);
  // reuse the context WITHOUT prepCanvas() — setting canvas.width clears it,
  // which would wipe the chart we just drew (that was the disappearing bug).
  const ctx = cv.getContext("2d"); const { X, Y } = XY(c);
  const px = X(tx);
  ctx.strokeStyle = C.muted; ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(px, c.padT - 4); ctx.lineTo(px, c.h - c.padB); ctx.stroke(); ctx.setLineDash([]);
  let rows = "";
  for (const s of c.series) {
    const p = nearest(s.pts, tx); if (!p) continue;
    ctx.fillStyle = s.color; ctx.beginPath(); ctx.arc(X(p[0]), Y(p[1]), 3.5, 0, 7); ctx.fill();
    rows += `<div class="tt-r"><span class="dot" style="background:${s.color}"></span>${s.name}: <b>${c.yFmt(p[1])}</b></div>`;
  }
  tip.innerHTML = `<div class="tt-t">t = ${Math.round(tx)} s</div>${rows}`;
  tip.style.display = "block";
  let lx = e.clientX + 14, ly = e.clientY + 14;
  if (lx + tip.offsetWidth > window.innerWidth) lx = e.clientX - tip.offsetWidth - 14;
  if (ly + tip.offsetHeight > window.innerHeight) ly = e.clientY - tip.offsetHeight - 14;
  tip.style.left = lx + "px"; tip.style.top = ly + "px";
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
    { name: "p99", color: C.red, pts: histLat.map((p) => [p.t, p.p99]) }], (v) => v.toFixed(0) + " ms");
}

// ---------- query load controls ----------

function mixRow(p) {
  const on = p.weight > 0;
  const wt = on ? p.weight : 10;
  return `<div class="mixrow ${on ? "" : "off"}" data-prof="${p.name}">
    <input type="checkbox" class="mx-on" ${on ? "checked" : ""} />
    <div class="nm">${p.name}<small>${escapeHtml(p.desc)} · <span class="rc">${fmtNum(p.count)}</span> run</small></div>
    <input type="number" class="mx-wt" min="1" value="${wt}" ${on ? "" : "disabled"} />
  </div>`;
}
let paused = false;
function applyPausedUI() {
  const btn = document.getElementById("c-pause");
  btn.textContent = paused ? "▶ Resume" : "⏸ Pause";
  btn.classList.toggle("primary", paused);
  const b = document.getElementById("badge-paused");
  b.style.display = paused ? "" : "none";
}
async function loadControl() {
  try {
    const c = await getJSON("/api/control");
    document.getElementById("c-conc").value = c.concurrency;
    document.getElementById("c-conc-max").textContent = "max " + c.max_workers;
    document.getElementById("c-rate").value = c.rate;
    document.getElementById("c-timeout").value = c.timeout_ms;
    document.getElementById("c-limit").value = c.limit;
    paused = c.paused; applyPausedUI();
    document.getElementById("mix").innerHTML = c.profiles.map(mixRow).join("");
    document.querySelectorAll("#mix .mx-on").forEach((cb) => cb.addEventListener("change", () => {
      const row = cb.closest(".mixrow"); const wt = row.querySelector(".mx-wt");
      row.classList.toggle("off", !cb.checked); wt.disabled = !cb.checked;
    }));
  } catch (e) {}
}
async function refreshCounts() {
  try {
    const c = await getJSON("/api/control");
    for (const p of c.profiles) {
      const row = document.querySelector(`#mix .mixrow[data-prof="${p.name}"] .rc`);
      if (row) row.textContent = fmtNum(p.count);
    }
  } catch (e) {}
}
function collectProfiles() {
  return [...document.querySelectorAll("#mix .mixrow")].map((row) => ({
    name: row.dataset.prof,
    weight: row.querySelector(".mx-on").checked ? (parseInt(row.querySelector(".mx-wt").value, 10) || 1) : 0,
  }));
}
async function applyControl() {
  const profiles = collectProfiles();
  if (!profiles.some((p) => p.weight > 0)) { alert("enable at least one query type"); return; }
  const body = {
    concurrency: parseInt(document.getElementById("c-conc").value, 10) || 1,
    rate: parseInt(document.getElementById("c-rate").value, 10) || 0,
    timeout_ms: parseInt(document.getElementById("c-timeout").value, 10) || 0,
    limit: parseInt(document.getElementById("c-limit").value, 10) || 20,
    profiles,
  };
  try { await postJSON("/api/control", body); loadControl(); } catch (e) {}
}
async function randomizeNow() {
  const btn = document.getElementById("c-randomize");
  btn.disabled = true; const prev = btn.textContent; btn.textContent = "🎲 re-rolling…";
  try { await postJSON("/api/randomize", {}); await refreshFeed(); }
  catch (e) {}
  finally { btn.disabled = false; btn.textContent = prev; }
}
async function pauseToggle() {
  paused = !paused; applyPausedUI();
  try { await postJSON("/api/control", { paused }); } catch (e) {}
}

// ---------- conversations + inline inspect search ----------

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
  const secs = m.ttl_ms > 0 ? Math.round(m.ttl_ms / 1000) : -1, plan = (m.plan || "").toLowerCase();
  return `<div class="msg" data-ttl="${m.ttl_ms}" data-fetched="${Date.now()}">
    <div class="meta"><span class="pill ${plan}">${m.plan || "?"}</span><span>${m.channel}</span><span>·</span><span>${m.user}</span><span>·</span><span>${m.thread}</span>
      <span class="ttl">⏳ <b class="ttlv">${secs < 0 ? "no ttl" : secs + "s"}</b></span></div>
    <div class="body">${escapeHtml(m.body)}</div><div class="ttlbar"><i style="width:100%"></i></div></div>`;
}
async function refreshMessages() {
  if (!selectedChannel) return;
  const q = document.getElementById("s-text").value.trim();
  const url = q ? ("/api/search?channel=" + encodeURIComponent(selectedChannel) + "&q=" + encodeURIComponent(q) + "&limit=30")
                : ("/api/messages?channel=" + encodeURIComponent(selectedChannel) + "&limit=30");
  try {
    const r = await getJSON(url);
    document.getElementById("s-query").textContent = r.query ? ("FT.SEARCH " + JSON.stringify(r.query) + "  →  total " + r.total) : "";
    const el = document.getElementById("messages");
    if (r.error) { el.innerHTML = `<div class="muted">error: ${escapeHtml(r.error)}</div>`; return; }
    el.innerHTML = (r.messages && r.messages.length) ? r.messages.map(msgCard).join("") : `<div class="muted">no messages (total match ${r.total || 0})</div>`;
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

// ---------- config + boot ----------

async function loadConfig() {
  try {
    const c = await getJSON("/api/config");
    document.getElementById("cfg-sub").textContent =
      `index "${c.index}" @ ${c.addr} · ${fmtNum(c.channels)} channels · ${fmtNum(c.users)} users` + (c.vocab_high ? ` · vocab+${fmtNum(c.vocab_high)}` : "");
    document.getElementById("tiers").textContent = "retention tiers: " + c.tiers.map((t) => `${t.name} (${t.ttl}, w${t.weight})`).join("  ·  ");
  } catch (e) {}
}
let feedData = [];
const feedSort = { k: "t", dir: -1 }; // default: newest first

function renderFeed() {
  const body = document.getElementById("feedbody");
  if (!feedData.length) { body.innerHTML = '<tr><td colspan="5" class="muted">waiting for queries…</td></tr>'; return; }
  const k = feedSort.k, dir = feedSort.dir;
  const rows = feedData.slice().sort((a, b) => {
    let av = a[k], bv = b[k];
    if (k === "query" || k === "profile") { av = (av || "").toString(); bv = (bv || "").toString(); return dir * av.localeCompare(bv); }
    return dir * ((av || 0) - (bv || 0));
  });
  body.innerHTML = rows.map((q) => {
    const cls = (q.err ? "err " : "") + (q.ms >= 500 ? "slow" : "");
    const t = q.err ? "ERR" : fmtNum(q.total);
    return `<tr class="${cls}"><td>${q.t.toFixed(0)}</td>` +
      `<td class="fp ${q.profile}">${q.profile}</td>` +
      `<td class="fcmd">${escapeHtml(q.query)}</td>` +
      `<td class="ft" title="${q.err ? escapeHtml(q.err) : "matches/rows"}">${t}</td>` +
      `<td class="fm">${q.ms.toFixed(1)}</td></tr>`;
  }).join("");
  document.querySelectorAll(".feedtbl th.sortable").forEach((th) => {
    const base = th.dataset.k === "t" ? "t (s)" : th.dataset.k === "profile" ? "type" : th.dataset.k === "query" ? "query" : th.dataset.k === "total" ? "matches" : "ms";
    th.innerHTML = base + (th.dataset.k === k ? ` <span class="arrow">${dir < 0 ? "▼" : "▲"}</span>` : "");
  });
}
async function refreshFeed() {
  try { feedData = await getJSON("/api/recent-queries"); renderFeed(); } catch (e) {}
}
document.querySelectorAll(".feedtbl th.sortable").forEach((th) =>
  th.addEventListener("click", () => {
    const k = th.dataset.k;
    if (feedSort.k === k) feedSort.dir *= -1; else { feedSort.k = k; feedSort.dir = (k === "query" || k === "profile") ? 1 : -1; }
    renderFeed();
  }));

async function tickStats() { try { renderStats(await getJSON("/api/stats")); } catch (e) {} }

loadConfig(); loadControl(); tickStats(); refreshChannels(); refreshMessages(); refreshFeed();
document.getElementById("c-apply").addEventListener("click", applyControl);
document.getElementById("c-randomize").addEventListener("click", randomizeNow);
document.getElementById("c-pause").addEventListener("click", pauseToggle);
document.getElementById("s-run").addEventListener("click", refreshMessages);
document.getElementById("s-clear").addEventListener("click", () => { document.getElementById("s-text").value = ""; refreshMessages(); });
document.getElementById("s-text").addEventListener("keydown", (e) => { if (e.key === "Enter") refreshMessages(); });

setInterval(tickStats, 2000);
setInterval(refreshChannels, 6000);
setInterval(refreshMessages, 3000);
setInterval(tickTTLs, 1000);
setInterval(refreshCounts, 3000);
setInterval(refreshFeed, 1500);
