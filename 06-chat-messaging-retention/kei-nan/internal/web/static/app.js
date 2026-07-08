"use strict";

const C = {
  blue: "#4c8dff", green: "#35c28f", amber: "#f2b134", red: "#ef5f6b",
  purple: "#a97bff", line: "#262b36", muted: "#9aa4b2", text: "#e6e8eb",
};

let selectedChannel = null;
const histRate = [];   // {t, ingest, query}
const histLat = [];    // {t, p50, p99}
const HIST_MAX = 600;

async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(r.status + " " + url);
  return r.json();
}

const fmtNum = (n) => {
  if (n == null) return "—";
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(Math.round(n));
};
const fmtBytes = (n) => {
  if (!n) return "0B";
  const u = ["B", "KB", "MB", "GB", "TB"]; let i = 0; let f = n;
  while (f >= 1024 && i < u.length - 1) { f /= 1024; i++; }
  return f.toFixed(1) + u[i];
};

// ---------- KPIs + badges ----------

function kpi(k, v, u) {
  return `<div class="kpi"><div class="k">${k}</div><div class="v">${v}<span class="u"> ${u || ""}</span></div></div>`;
}

function renderStats(s) {
  const foot = s.disk_mode ? s.latest.disk_usage : s.latest.used_mem;
  document.getElementById("kpis").innerHTML = [
    kpi("ingest", fmtNum(s.rates.ingest), "/s"),
    kpi("query", fmtNum(s.rates.query), "/s"),
    kpi("indexed docs", fmtNum(s.latest.num_docs), ""),
    kpi("index records", fmtNum(s.latest.num_records), ""),
    kpi(s.disk_mode ? "on-disk" : "used mem", fmtBytes(foot), ""),
    kpi("p50 / p99", s.latency.p50.toFixed(1) + " / " + s.latency.p99.toFixed(1), "ms"),
    kpi("stale hits", fmtNum(s.stale_hits), ""),
    kpi("tracked", fmtNum(s.oracle.tracked), ""),
  ].join("");

  document.getElementById("badge-elapsed").textContent = "t=" + Math.round(s.elapsed_sec) + "s";
  const mode = document.getElementById("badge-mode");
  mode.textContent = s.disk_mode ? "disk-backed index" : "in-RAM index";
  mode.className = "badge " + (s.disk_mode ? "ok" : "warn");

  const cor = document.getElementById("badge-correct");
  if (s.stale_hits > 0) { cor.textContent = "STALE HITS: " + s.stale_hits; cor.className = "badge bad"; }
  else { cor.textContent = "no stale hits ✓"; cor.className = "badge ok"; }

  const pl = document.getElementById("badge-plateau");
  if (s.verdict.samples < 6) { pl.textContent = "footprint: warming up"; pl.className = "badge ghost"; }
  else if (s.verdict.plateau) { pl.textContent = "footprint: PLATEAU"; pl.className = "badge ok"; }
  else { pl.textContent = "footprint: GROWING"; pl.className = "badge warn"; }
  document.getElementById("verdict-note").textContent = s.verdict.note || "";

  // accumulate client-side history for rate/latency charts
  histRate.push({ t: s.elapsed_sec, ingest: s.rates.ingest, query: s.rates.query });
  histLat.push({ t: s.elapsed_sec, p50: s.latency.p50, p99: s.latency.p99 });
  if (histRate.length > HIST_MAX) histRate.shift();
  if (histLat.length > HIST_MAX) histLat.shift();

  drawCharts(s);
}

// ---------- charts ----------

function prepCanvas(cv) {
  const dpr = window.devicePixelRatio || 1;
  const w = cv.clientWidth, h = cv.clientHeight || 180;
  cv.width = w * dpr; cv.height = h * dpr;
  const ctx = cv.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w, h };
}

function drawLine(cv, series, yFmt) {
  const { ctx, w, h } = prepCanvas(cv);
  ctx.clearRect(0, 0, w, h);
  const padL = 52, padR = 10, padT = 10, padB = 22;
  const allPts = series.flatMap((s) => s.pts);
  if (allPts.length < 2) {
    ctx.fillStyle = C.muted; ctx.font = "12px sans-serif";
    ctx.fillText("collecting…", padL, h / 2);
    return;
  }
  let xmin = Infinity, xmax = -Infinity, ymin = 0, ymax = -Infinity;
  for (const p of allPts) { xmin = Math.min(xmin, p[0]); xmax = Math.max(xmax, p[0]); ymax = Math.max(ymax, p[1]); }
  if (xmax === xmin) xmax = xmin + 1;
  if (ymax <= 0) ymax = 1;
  ymax *= 1.1;
  const X = (x) => padL + ((x - xmin) / (xmax - xmin)) * (w - padL - padR);
  const Y = (y) => h - padB - (y / ymax) * (h - padT - padB);

  // gridlines + y labels
  ctx.strokeStyle = C.line; ctx.fillStyle = C.muted; ctx.font = "10px sans-serif"; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const yv = (ymax / 4) * i, y = Y(yv);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke();
    ctx.fillText(yFmt(yv), 4, y + 3);
  }
  // x labels (start/end seconds)
  ctx.fillText(Math.round(xmin) + "s", padL, h - 6);
  ctx.fillText(Math.round(xmax) + "s", w - padR - 24, h - 6);

  // lines
  for (const s of series) {
    ctx.strokeStyle = s.color; ctx.lineWidth = 2; ctx.beginPath();
    s.pts.forEach((p, i) => { const x = X(p[0]), y = Y(p[1]); i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); });
    ctx.stroke();
  }
  // legend
  let lx = padL;
  ctx.font = "11px sans-serif";
  for (const s of series) {
    ctx.fillStyle = s.color; ctx.fillRect(lx, padT - 4, 10, 10);
    ctx.fillStyle = C.text; ctx.fillText(s.name, lx + 14, padT + 5);
    lx += 22 + ctx.measureText(s.name).width;
  }
}

function drawCharts(s) {
  const series = s.series || [];
  const useDisk = s.disk_mode;
  document.getElementById("cap-foot").textContent = useDisk ? "on-disk footprint (search_disk_usage)" : "process memory (used_memory)";
  drawLine(document.getElementById("c-foot"),
    [{ name: useDisk ? "disk" : "mem", color: C.blue, pts: series.map((p) => [p.t, useDisk ? p.disk_usage : p.used_mem]) }],
    (v) => fmtBytes(v));
  drawLine(document.getElementById("c-docs"),
    [{ name: "num_docs", color: C.green, pts: series.map((p) => [p.t, p.num_docs]) },
     { name: "num_records", color: C.amber, pts: series.map((p) => [p.t, p.num_records]) }],
    (v) => fmtNum(v));
  drawLine(document.getElementById("c-rate"),
    [{ name: "ingest/s", color: C.green, pts: histRate.map((p) => [p.t, p.ingest]) },
     { name: "query/s", color: C.blue, pts: histRate.map((p) => [p.t, p.query]) }],
    (v) => fmtNum(v));
  drawLine(document.getElementById("c-lat"),
    [{ name: "p50", color: C.amber, pts: histLat.map((p) => [p.t, p.p50]) },
     { name: "p99", color: C.red, pts: histLat.map((p) => [p.t, p.p99]) }],
    (v) => v.toFixed(0));
}

// ---------- conversations ----------

async function refreshChannels() {
  try {
    const chans = await getJSON("/api/channels");
    const el = document.getElementById("channels");
    if (!chans.length) { el.innerHTML = '<div class="muted">no live channels yet…</div>'; return; }
    if (!selectedChannel) selectedChannel = chans[0].channel;
    el.innerHTML = chans.map((c) =>
      `<div class="chan ${c.channel === selectedChannel ? "active" : ""}" data-ch="${c.channel}">
         <span>${c.channel}</span><span class="cnt">${fmtNum(c.count)}</span></div>`).join("");
    el.querySelectorAll(".chan").forEach((n) =>
      n.addEventListener("click", () => { selectedChannel = n.dataset.ch; refreshChannels(); refreshMessages(); }));
  } catch (e) { /* ignore transient */ }
}

function msgCard(m) {
  const secs = m.ttl_ms > 0 ? Math.round(m.ttl_ms / 1000) : -1;
  const ttlTxt = secs < 0 ? "no ttl" : secs + "s";
  const plan = (m.plan || "").toLowerCase();
  return `<div class="msg" data-ttl="${m.ttl_ms}" data-fetched="${Date.now()}">
    <div class="meta">
      <span class="pill ${plan}">${m.plan || "?"}</span>
      <span>${m.channel}</span><span>·</span><span>${m.user}</span><span>·</span><span>${m.thread}</span>
      <span class="ttl">⏳ <b class="ttlv">${ttlTxt}</b></span>
    </div>
    <div class="body">${escapeHtml(m.body)}</div>
    <div class="ttlbar"><i style="width:100%"></i></div>
  </div>`;
}

async function refreshMessages() {
  if (!selectedChannel) return;
  try {
    const res = await getJSON("/api/messages?channel=" + encodeURIComponent(selectedChannel) + "&limit=30");
    const el = document.getElementById("messages");
    if (!res.messages || !res.messages.length) {
      el.innerHTML = `<div class="muted">no messages in ${selectedChannel} right now (total match ${res.total})</div>`;
      return;
    }
    el.innerHTML = res.messages.map(msgCard).join("");
  } catch (e) { /* ignore */ }
}

// live TTL countdown between fetches
function tickTTLs() {
  document.querySelectorAll(".msg").forEach((n) => {
    const ttl0 = parseInt(n.dataset.ttl, 10);
    if (isNaN(ttl0) || ttl0 < 0) return;
    const elapsed = Date.now() - parseInt(n.dataset.fetched, 10);
    const rem = ttl0 - elapsed;
    const v = n.querySelector(".ttlv");
    const bar = n.querySelector(".ttlbar > i");
    if (rem <= 0) { if (v) v.textContent = "expiring…"; if (bar) { bar.style.width = "0%"; bar.style.background = C.red; } n.style.opacity = "0.5"; return; }
    if (v) v.textContent = Math.ceil(rem / 1000) + "s";
    if (bar) bar.style.width = Math.max(0, Math.min(100, (rem / ttl0) * 100)) + "%";
  });
}

// ---------- search ----------

async function runSearch() {
  const p = new URLSearchParams();
  const ch = document.getElementById("s-channel").value.trim();
  const us = document.getElementById("s-user").value.trim();
  const th = document.getElementById("s-thread").value.trim();
  const tx = document.getElementById("s-text").value.trim();
  if (ch) p.set("channel", ch); if (us) p.set("user", us); if (th) p.set("thread", th); if (tx) p.set("q", tx);
  p.set("limit", "30");
  try {
    const res = await getJSON("/api/search?" + p.toString());
    document.getElementById("s-query").textContent = "FT.SEARCH " + JSON.stringify(res.query) + "  →  total " + res.total;
    const el = document.getElementById("s-results");
    if (res.error) { el.innerHTML = `<div class="muted">error: ${escapeHtml(res.error)}</div>`; return; }
    if (!res.messages || !res.messages.length) { el.innerHTML = `<div class="muted">no matches</div>`; return; }
    el.innerHTML = res.messages.map(msgCard).join("");
  } catch (e) {
    document.getElementById("s-results").innerHTML = `<div class="muted">request failed</div>`;
  }
}

// ---------- config ----------

async function loadConfig() {
  try {
    const c = await getJSON("/api/config");
    document.getElementById("cfg-sub").textContent =
      `index "${c.index}" @ ${c.addr} · ${fmtNum(c.channels)} channels · ${fmtNum(c.users)} users`;
    document.getElementById("tiers").textContent =
      "retention tiers: " + c.tiers.map((t) => `${t.name} (${t.ttl}, w${t.weight})`).join("  ·  ");
  } catch (e) { /* ignore */ }
}

function escapeHtml(s) {
  return (s || "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

async function tickStats() { try { renderStats(await getJSON("/api/stats")); } catch (e) {} }

// ---------- boot ----------

loadConfig();
tickStats();
refreshChannels();
refreshMessages();
document.getElementById("s-run").addEventListener("click", runSearch);
document.getElementById("s-text").addEventListener("keydown", (e) => { if (e.key === "Enter") runSearch(); });

setInterval(tickStats, 2000);
setInterval(refreshChannels, 6000);
setInterval(refreshMessages, 3000);
setInterval(tickTTLs, 1000);
window.addEventListener("resize", () => { /* charts redraw on next tick */ });
