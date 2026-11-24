// Draws the measurements recorded in production (data/production.json).
"use strict";

// Charts are drawn in fixed logical units (the canvas attributes) at the
// screen's real pixel density, and redrawn when the size or theme changes.
const charts = [];

function lineChart(canvas, lines, opts) {
  if (!charts.some((c) => c.canvas === canvas)) charts.push({ canvas, lines, opts });
  if (!canvas._w) { canvas._w = canvas.width; canvas._h = canvas.height; }
  const dpr = window.devicePixelRatio || 1;
  const cssW = canvas.getBoundingClientRect().width || canvas._w;
  canvas.width = Math.round(cssW * dpr);
  canvas.height = Math.round(cssW * dpr * canvas._h / canvas._w);
  const ctx = canvas.getContext("2d");
  ctx.setTransform(canvas.width / canvas._w, 0, 0, canvas.width / canvas._w, 0, 0);
  const W = canvas._w, H = canvas._h, padL = 44, padB = 26, padT = 14, padR = 12;
  const css = (n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
  const pts = lines.flatMap((l) => l.points.filter((p) => p[1] !== null));
  if (!pts.length) return;
  const t0 = Math.min(...pts.map((p) => p[0])), t1 = Math.max(...pts.map((p) => p[0]));
  const lo = opts.min, hi = opts.max !== undefined ? opts.max : Math.max(...pts.map((p) => p[1])) * 1.1 || 1;
  const x = (t) => padL + (W - padL - padR) * (t - t0) / (t1 - t0 || 1);
  const y = (v) => padT + (H - padT - padB) * (1 - (Math.min(v, hi) - lo) / (hi - lo));
  ctx.clearRect(0, 0, W, H);
  ctx.font = "12px system-ui, sans-serif";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const v = lo + (hi - lo) * i / 4;
    ctx.strokeStyle = css("--rule");
    ctx.beginPath(); ctx.moveTo(padL, y(v)); ctx.lineTo(W - padR, y(v)); ctx.stroke();
    ctx.fillStyle = css("--soft");
    ctx.fillText(opts.fmt(v), 6, y(v) + 4);
  }
  for (let d = Math.ceil(t0 / 86400) * 86400; d <= t1; d += 86400) {
    const label = new Date(d * 1000).toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
    ctx.fillText(label, x(d) - 14, H - 8);
  }
  for (const l of lines) {
    ctx.strokeStyle = css(l.color);
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    let pen = false;
    for (const [t, v] of l.points) {
      if (v === null) { pen = false; continue; }
      pen ? ctx.lineTo(x(t), y(v)) : ctx.moveTo(x(t), y(v));
      pen = true;
    }
    ctx.stroke();
  }
}

fetch("data/production.json").then((r) => r.json()).then((d) => {
  const first = (o) => Object.values(o)[0];
  const rq = d.totals.requestsByOutcome7d;
  const sum = (outcome) => (rq[`op=put,outcome=${outcome}`] || 0) + (rq[`op=get,outcome=${outcome}`] || 0);
  const n = (v) => Math.round(v).toLocaleString("en");
  const stat = (big, small) => `<div class="stat"><b>${big}</b><span>${small}</span></div>`;
  document.getElementById("stats").innerHTML =
    stat(n(sum("ok")), "requests answered, half of them writes") +
    stat(n(sum("timeout")), "requests that timed out") +
    stat(n(first(d.totals.leaderChanges7d)), "leader changes, most of them forced by the script");
  const ms = (v) => (v * 1000).toFixed(0);
  lineChart(document.getElementById("latChart"),
    [{ points: first(d.writeP99), color: "--candidate" }, { points: first(d.writeP50), color: "--leader" }],
    { min: 0, max: 0.06, fmt: ms });
  lineChart(document.getElementById("electChart"),
    [{ points: first(d.leaderChangesPerHour), color: "--link" }], { min: 0, fmt: (v) => v.toFixed(0) });
}).catch(() => { document.getElementById("stats").textContent = "The recorded data could not be loaded."; });

function redrawCharts() { for (const c of charts) lineChart(c.canvas, c.lines, c.opts); }
let resizeTimer = 0;
window.addEventListener("resize", () => { clearTimeout(resizeTimer); resizeTimer = setTimeout(redrawCharts, 150); });
window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", redrawCharts);
