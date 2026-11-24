// Drives the cluster simulation (window.quorum, from main.wasm) and draws it.
"use strict";

const $ = (id) => document.getElementById(id);
const SVG = "http://www.w3.org/2000/svg";
let state = null;
let selected = 1;
let lastStopped = 0;
let paused = false;
let speed = 1;
let carry = 0;
let lastFrame = 0;
let keyNo = 0;

const secs = (ms) => (ms / 1000).toFixed(1) + " s";
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const num = (n) => n.toLocaleString("en");

// The page redraws about 60 times a second. These only touch the DOM when the
// content really changes, so text stays selectable and nothing flickers.
function setText(node, text) { if (node.textContent !== text) node.textContent = text; }
function setHTML(node, html) { if (node._html !== html) { node.innerHTML = html; node._html = html; } }
function setClass(node, cls) { if (node.getAttribute("class") !== cls) node.setAttribute("class", cls); }

// Five nodes on a circle, node 1 at the top.
function pos(id, n) {
  const a = -Math.PI / 2 + (2 * Math.PI * (id - 1)) / n;
  return [200 + 140 * Math.cos(a), 185 + 140 * Math.sin(a)];
}

function el(name, attrs, parent) {
  const e = document.createElementNS(SVG, name);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(e);
  return e;
}

// The nodes are created once and only updated afterwards: recreating them on
// every frame would replace the element under the mouse between press and
// release, and the click would never register.
let ringNodes = null;

function select(id) {
  selected = id;
  render();
}

function buildRing() {
  const svg = $("ring");
  const n = state.nodes.length;
  svg.innerHTML = "";
  el("g", { id: "edges" }, svg);
  ringNodes = state.nodes.map((node) => {
    const [x, y] = pos(node.id, n);
    const g = el("g", { class: "node", tabindex: "0", role: "button", "aria-label": `Node ${node.id}` }, svg);
    const sel = el("circle", { cx: x, cy: y, r: 35, class: "sel" }, g);
    const ring = el("circle", { cx: x, cy: y, r: 40, class: "ring" }, g);
    el("circle", { cx: x, cy: y, r: 28 }, g);
    el("text", { x, y: y + 8, class: "num" }, g).textContent = node.id;
    const tag = el("text", { x, y: y + 56, class: "tag" }, g);
    const cut = el("text", { x, y: y + 71, class: "tag bad" }, g);
    g.addEventListener("click", () => select(node.id));
    g.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); select(node.id); }
    });
    return { g, sel, ring, tag, cut };
  });
}

function drawRing() {
  if (!ringNodes || ringNodes.length !== state.nodes.length) buildRing();
  const n = state.nodes.length;
  // One line per pair of nodes, styled by the most notable message kind on it.
  const rank = { append: 1, vote: 2, snapshot: 3 };
  const pairs = {};
  for (const f of state.flows) {
    const k = Math.min(f.from, f.to) + "-" + Math.max(f.from, f.to);
    if (!pairs[k] || rank[f.kind] > rank[pairs[k]]) pairs[k] = f.kind;
  }
  const edges = $("edges");
  const key = JSON.stringify(pairs);
  if (edges._key !== key) {
    edges._key = key;
    edges.innerHTML = "";
    for (const k in pairs) {
      const [a, b] = k.split("-").map(Number);
      const [x1, y1] = pos(a, n), [x2, y2] = pos(b, n);
      el("line", { x1, y1, x2, y2, class: "edge " + pairs[k] }, edges);
    }
  }
  state.nodes.forEach((node, i) => {
    const r = ringNodes[i];
    setClass(r.g, `node ${node.role}${node.isolated ? " cut" : ""}${node.id === selected ? " selected" : ""}`);
    r.sel.style.display = node.id === selected ? "" : "none";
    r.ring.style.display = node.isolated ? "" : "none";
    const label = node.role === "down" ? "stopped" : node.role === "pre-candidate" ? "asking" : node.role;
    setText(r.tag, node.alive ? `${label}, term ${node.term}` : label);
    setText(r.cut, node.isolated ? "disconnected" : "");
  });
}

// One plain sentence about what the cluster is doing right now.
function caption() {
  const up = state.nodes.filter((x) => x.alive && !x.isolated).length;
  const voting = state.flows.some((f) => f.kind === "vote");
  if (state.violation) return "The safety check failed. This should never happen; please report it.";
  if (state.leader) {
    const l = state.nodes[state.leader - 1];
    let s = `Node ${l.id} is the leader for term ${l.term}. It sends a heartbeat to the others every 30 ms.`;
    if (up < 3) s += " Fewer than three nodes can reach it, so new writes cannot be committed.";
    else if (up < 5) s += ` ${up} of 5 nodes are reachable, enough to keep committing writes.`;
    return s;
  }
  if (up < 3) return "No leader, and fewer than three nodes can talk to each other. No election can succeed until more nodes are back.";
  return voting
    ? "No leader right now. An election is going on (orange lines)."
    : "No leader right now. The nodes will notice the silence within 150 to 300 ms and hold an election.";
}

function renderPanel() {
  const n = state.nodes[selected - 1];
  setText($("panelTitle"), `Node ${n.id}`);
  setText($("power"), n.alive ? "Stop" : "Start");
  setText($("net"), n.isolated ? "Reconnect" : "Disconnect");
  setHTML($("facts"),
    `<dt>Role</dt><dd>${n.alive ? esc(n.role) : "stopped"}</dd>` +
    `<dt>Term</dt><dd>${n.term}</dd>` +
    `<dt>Committed up to</dt><dd>index ${n.commit}</dd>`);
  setText($("disk"), "Saved on disk: " + n.disk + ".");
  setHTML($("log"), (n.tail || []).slice().reverse()
    .map((e) => `<tr><td>${e.index}</td><td>${e.term}</td><td>${esc(e.what)}</td></tr>`).join("")
    || `<tr><td colspan="3">Empty</td></tr>`);
  const keys = Object.entries(n.keys);
  setText($("keys"), keys.length ? "Keys: " + keys.map(([k, v]) => `${k}=${v}`).join(", ") : "No keys yet.");
}

function renderEvents() {
  setHTML($("events"), state.events.slice().reverse()
    .map((e) => `<li class="ev-${esc(e.kind)}"><time>${secs(e.ms)}</time><span>${esc(e.text)}</span></li>`).join(""));
}

// Buttons that cannot do anything right now are disabled, with the reason as
// a tooltip, instead of silently doing nothing.
function renderButtons() {
  const noLeader = !state.leader;
  const put = $("put"), kill = $("killLeader");
  put.disabled = noLeader;
  put.title = noLeader ? "There is no leader to accept a write right now" : "";
  kill.disabled = noLeader;
  kill.title = noLeader ? "There is no leader right now" : "";
  const r = $("restartLast");
  const stopped = lastStopped && !state.nodes[lastStopped - 1].alive;
  r.disabled = !stopped;
  setText(r, stopped ? `Restart node ${lastStopped}` : "Restart it");
}

function render() {
  drawRing();
  setText($("caption"), caption());
  const meta = $("meta");
  setClass(meta, state.violation ? "meta bad" : "meta");
  setText(meta, state.violation
    ? "Safety violation: " + state.violation
    : `${secs(state.ms)} simulated. Safety checked ${num(state.checks)} times, no violation. ${num(state.delivered)} messages delivered, ${num(state.dropped)} not delivered.`);
  renderPanel();
  renderEvents();
  renderButtons();
}

function refresh() {
  state = JSON.parse(quorum.state());
  render();
}

function frame(t) {
  if (!lastFrame) lastFrame = t;
  const dt = Math.min(t - lastFrame, 250); // after a hidden tab comes back, do not run minutes at once
  lastFrame = t;
  if (!paused) {
    carry += (dt / state.tickMs) * speed;
    const n = Math.floor(carry);
    carry -= n;
    if (n > 0) quorum.step(n);
  }
  refresh();
  requestAnimationFrame(frame);
}

function wire() {
  $("put").onclick = () => {
    if (quorum.put("k" + (keyNo + 1), String((keyNo + 1) * 7))) keyNo++;
    refresh();
  };
  $("killLeader").onclick = () => {
    if (!state.leader) return;
    lastStopped = selected = state.leader;
    quorum.kill(state.leader);
    refresh();
  };
  $("restartLast").onclick = () => { if (lastStopped) quorum.restart(lastStopped); refresh(); };
  $("power").onclick = () => {
    const n = state.nodes[selected - 1];
    if (n.alive) { lastStopped = n.id; quorum.kill(n.id); } else quorum.restart(n.id);
    refresh();
  };
  $("net").onclick = () => { const n = state.nodes[selected - 1]; quorum.isolate(n.id, !n.isolated); refresh(); };
  $("loss").oninput = (e) => { quorum.setDropRate(e.target.value / 100); $("lossv").textContent = e.target.value + "%"; };
  $("speed").onchange = (e) => { speed = parseFloat(e.target.value); };
  $("pause").onclick = (e) => { paused = !paused; e.target.textContent = paused ? "Resume" : "Pause"; };
  $("reset").onclick = () => {
    quorum.reset(Math.floor(Math.random() * 1e6));
    quorum.setDropRate($("loss").value / 100);
    keyNo = 0; selected = 1; lastStopped = 0; refresh();
  };
}

window.addEventListener("quorum-ready", () => { wire(); refresh(); requestAnimationFrame(frame); });

const go = new Go();
WebAssembly.instantiateStreaming(fetch("main.wasm"), go.importObject)
  .then((r) => go.run(r.instance))
  .catch((err) => { $("caption").textContent = "The simulation could not load in this browser (" + err + ")."; });
