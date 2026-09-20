/* sipbot Answer Test UI */
const $ = (sel, el = document) => el.querySelector(sel);
const $$ = (sel, el = document) => [...el.querySelectorAll(sel)];

async function api(path, opts) {
  const res = await fetch(path, opts);
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  return res.json();
}

// ── Tabs ──
$$(".tab").forEach(btn => {
  btn.addEventListener("click", () => {
    $$(".tab").forEach(b => b.classList.toggle("active", b === btn));
    $$(".tab-page").forEach(p => p.classList.toggle("active", p.id === `tab-${btn.dataset.tab}`));
  });
});

// --- Strategies tab: accounts + strategies ---
let strategyNames = []; // for binding dropdowns

async function loadAccounts() {
  try {
    const [accData, stratData] = await Promise.all([
      api("/api/accounts"),
      api("/api/strategies"),
    ]);
    strategyNames = (stratData.strategies || []).map(s => s.name);
    refreshFilterOptions((accData.accounts || []).map(a => a.username), strategyNames);
    const list = $("#account-list");
    list.innerHTML = "";
    for (const a of accData.accounts || []) {
      const reg = a.registration;
      const regBadge = a.register
        ? (reg && reg.registered
          ? `<span class="badge ok">registered${reg.expires ? " " + reg.expires + "s" : ""}</span>`
          : `<span class="badge err">NOT registered</span>`)
        : `<span class="badge">no reg</span>`;
      const bound = a.strategy
        ? `<span class="badge bound">⚡ ${esc(a.strategy)}</span>`
        : (a.strategy_inline ? `<span class="badge warn">inline (toml)</span>` : "");
      const bindOptions = ["", ...strategyNames].map(n =>
        `<option value="${esc(n)}" ${n === (a.strategy || "") ? "selected" : ""}>${n ? esc(n) : "(unbound)"}</option>`).join("");
      const el = document.createElement("div");
      el.className = "card account-card";
      el.innerHTML = `
        <h3>${esc(a.username)}@${esc(a.domain)}</h3>
        <div class="meta">
          <span class="badge info">${esc(a.transport)}</span>
          ${regBadge}
          ${bound}
        </div>
        <div class="strategy">${esc(a.summary || "default answer")}</div>
        <div class="card-actions">
          <select class="bind-select" title="Bind strategy">${bindOptions}</select>
          <button class="btn act-copy" title="Duplicate account">Copy</button>
          <button class="btn act-edit" title="Edit account">Edit</button>
        </div>
      `;
      $(".bind-select", el).addEventListener("change", async (ev) => {
        await postJSON("/api/accounts/bind", {
          username: a.username, domain: a.domain, strategy: ev.target.value,
        });
        loadAccounts();
      });
      $(".act-copy", el).addEventListener("click", async () => {
        await postJSON("/api/accounts/copy", { username: a.username, domain: a.domain });
        loadAccounts();
      });
      $(".act-edit", el).addEventListener("click", () => openAccountModal(a));
      list.appendChild(el);
    }
  } catch (e) {
    console.error("loadAccounts", e);
  }
}

async function loadStrategies() {
  try {
    const data = await api("/api/strategies");
    const list = $("#strategy-list");
    list.innerHTML = "";
    for (const s of data.strategies || []) {
      const el = document.createElement("div");
      el.className = "card strategy-card";
      const bound = (s.bound_accounts || []).map(u => `<span class="badge ok">${esc(u)}</span>`).join(" ");
      el.innerHTML = `
        <h3>${esc(s.name)}</h3>
        <div class="meta">
          ${s.match_caller ? `<span class="badge warn">caller ${esc(s.match_caller)}</span>` : ""}
          ${s.codecs ? `<span class="badge">${esc(s.codecs.join("/"))}</span>` : ""}
        </div>
        <div class="strategy">${esc(s.summary)}</div>
        <div class="bound-row">${bound ? "bound: " + bound : '<span class="muted">not bound</span>'}</div>
        <div class="card-actions">
          <button class="btn act-edit">Edit</button>
          <button class="btn act-copy">Copy</button>
          <button class="btn danger act-del">Delete</button>
        </div>
      `;
      $(".act-edit", el).addEventListener("click", () => openStrategyModal(s.name, s.strategy));
      $(".act-copy", el).addEventListener("click", async () => {
        await postJSON("/api/strategies/copy", { source: s.name });
        refreshStrategies();
      });
      $(".act-del", el).addEventListener("click", async () => {
        if (!confirm(`Delete strategy "${s.name}"? Bound accounts will fall back to the default answer.`)) return;
        await fetch(`/api/strategies/${encodeURIComponent(s.name)}`, { method: "DELETE" });
        refreshStrategies();
      });
      list.appendChild(el);
    }
  } catch (e) {
    console.error("loadStrategies", e);
  }
}

function refreshStrategies() {
  loadStrategies();
  loadAccounts();
}

async function postJSON(path, body) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) alert(`${path}: ${await res.text()}`);
  return res.json();
}

// --- Account editor ---
async function openAccountModal(account) {
  let config;
  try { config = await api("/api/config"); } catch (e) { alert("failed to load config: " + e); return; }
  const acc = (config.accounts || []).find(
    a => a.username === account.username && a.domain === account.domain);
  if (!acc) { alert("account not found"); return; }
  $("#am-title").textContent = `Account · ${acc.username}@${acc.domain}`;
  const f = $("#am-form");
  f.innerHTML = `
    <label>User name<input id="a-username" value="${esc(acc.username)}"></label>
    <label>Domain / Realm<input id="a-domain" value="${esc(acc.domain)}"></label>
    <label>Password<input id="a-password" type="password" value="${esc(acc.password || "")}"></label>
    <label>Registrar / proxy (host:port)<input id="a-proxy" value="${esc(acc.proxy || "")}" placeholder="optional"></label>
    <label class="inline"><input type="checkbox" id="a-register" ${acc.register ? "checked" : ""}> Register to server</label>
    <label>Transport
      <select id="a-transport">
        ${["udp","tcp","ws","wss"].map(t => `<option value="${t}" ${(acc.transport || "udp") === t ? "selected" : ""}>${t.toUpperCase()}</option>`).join("")}
      </select>
    </label>
    <label>Bind address (udp/tcp)<input id="a-transport-addr" placeholder="empty = global ${esc(config.addr || "0.0.0.0:35060")}" value="${esc(acc.transport_addr || "")}"></label>
    <label>WS URL (ws/wss)<input id="a-transport-ws" placeholder="empty = global ${esc(config.ws_url || "-")}" value="${esc(acc.transport_ws_url || "")}"></label>
    <label>Bound strategy
      <select id="a-strategy">
        <option value="">(unbound / default answer)</option>
        ${strategyNames.map(n => `<option ${n === (acc.strategy || "") ? "selected" : ""}>${esc(n)}</option>`).join("")}
      </select>
    </label>
  `;
  $("#account-modal").classList.remove("hidden");
  $("#am-close").onclick = () => $("#account-modal").classList.add("hidden");
  $("#am-save").onclick = async () => {
    acc.username = $("#a-username").value.trim() || acc.username;
    acc.domain = $("#a-domain").value.trim() || acc.domain;
    acc.password = $("#a-password").value || null;
    acc.proxy = $("#a-proxy").value.trim() || null;
    acc.register = $("#a-register").checked ? true : null;
    acc.transport = $("#a-transport").value;
    acc.transport_addr = $("#a-transport-addr").value.trim() || null;
    acc.transport_ws_url = $("#a-transport-ws").value.trim() || null;
    acc.strategy = $("#a-strategy").value || null;
    await putConfig(config);
    $("#account-modal").classList.add("hidden");
    refreshStrategies();
  };
}

// [New Account]
function initAddAccount() {
  $("#btn-add-account").addEventListener("click", () => openAccountCreate());
  async function openAccountCreate() {
    const config = await api("/api/config");
    const f = $("#am-form");
    $("#am-title").textContent = "New Account";
    f.innerHTML = `
      <label>User name<input id="a-username" placeholder="1003"></label>
      <label>Domain / Realm<input id="a-domain" value="${esc(config.accounts[0]?.domain || "127.0.0.1")}"></label>
      <label>Password<input id="a-password" type="password"></label>
      <label>Registrar / proxy<input id="a-proxy" placeholder="host:port (optional)"></label>
      <label class="inline"><input type="checkbox" id="a-register" checked> Register to server</label>
      <label>Transport
        <select id="a-transport">${["udp","tcp","ws","wss"].map(t => `<option value="${t}">${t.toUpperCase()}</option>`).join("")}</select>
      </label>
      <label>Bind address (udp/tcp)<input id="a-transport-addr" placeholder="empty = global"></label>
      <label>WS URL (ws/wss)<input id="a-transport-ws" placeholder="wss://host/ws"></label>
      <label>Bound strategy
        <select id="a-strategy">
          <option value="">(unbound / default answer)</option>
          ${strategyNames.map(n => `<option>${esc(n)}</option>`).join("")}
        </select>
      </label>
    `;
    $("#account-modal").classList.remove("hidden");
    $("#am-close").onclick = () => $("#account-modal").classList.add("hidden");
    $("#am-save").onclick = async () => {
      const username = $("#a-username").value.trim();
      if (!username) { alert("username is required"); return; }
      config.accounts.push({
        username,
        domain: $("#a-domain").value.trim() || "127.0.0.1",
        password: $("#a-password").value || null,
        proxy: $("#a-proxy").value.trim() || null,
        register: $("#a-register").checked ? true : null,
        transport: $("#a-transport").value,
        transport_addr: $("#a-transport-addr").value.trim() || null,
        transport_ws_url: $("#a-transport-ws").value.trim() || null,
        strategy: $("#a-strategy").value || null,
      });
      await putConfig(config);
      $("#account-modal").classList.add("hidden");
      refreshStrategies();
    };
  }
}

// --- Strategy editor ---
let editConfig = null; // full Config JSON being edited

const STRATEGY_TEMPLATES = {
  standard: { label: "Standard", build: () => ({ ring: { duration_secs: 3 }, answer: { action: "echo" }, hangup: { mode: "after", after_secs: 30 } }) },
  crbt: { label: "Ringback (early media)", build: () => ({ ring: { duration_secs: 4, ringback: "wavs/ringing.wav" }, answer: { action: "echo" }, hangup: { mode: "after", after_secs: 30 } }) },
  announce: { label: "Announce+Jump", build: () => ({ ring: { duration_secs: 1 }, announce: { file: "wavs/announce.wav", jump_after: true }, answer: { action: "echo" }, hangup: { mode: "after", after_secs: 30 } }) },
  reject: { label: "Reject", build: () => ({ reject: { code: 486, tone: "wavs/ringing.wav", delay_secs: 2 } }) },
  blank: { label: "Blank", build: () => ({}) },
};

async function openStrategyModal(name, strategyObj) {
  try { editConfig = await api("/api/config"); }
  catch (e) { alert("failed to load config: " + e); return; }
  $("#sm-title").textContent = `Strategy · ${name}`;

  const media = await api("/api/media");
  const mediaFiles = (media.media || []).map(m => m.name);

  const f = $("#sm-form");
  f.innerHTML = `
    <fieldset><legend>Basic</legend>
      <label>Strategy name (binding key)<input id="f-name" value="${esc(name)}"></label>
      <label>Caller match (138*|139*, empty = all)<input id="f-match-caller" value="${esc(strategyObj.match_caller || "")}"></label>
      <label>Codec priority (comma separated)<input id="f-codecs" placeholder="pcmu,pcma,g722,opus" value="${esc((strategyObj.codecs || []).join(","))}"></label>
    </fieldset>
    <fieldset><legend>Ringing Stage</legend>
      <label>Mode
        <select id="f-ring-mode">
          <option value="none" ${!strategyObj.ring ? "selected" : ""}>no ringing (answer immediately)</option>
          <option value="180" ${strategyObj.ring && !strategyObj.ring.ringback && strategyObj.ring.ringback !== "" ? "selected" : ""}>180 plain ringing</option>
          <option value="builtin" ${strategyObj.ring && strategyObj.ring.ringback === "" ? "selected" : ""}>183 + built-in ringback tone</option>
          <option value="file" ${strategyObj.ring && strategyObj.ring.ringback ? "selected" : ""}>183 + ringback file (early media)</option>
        </select>
      </label>
      <label>Ring duration (secs, 0 = answer immediately)<input id="f-ring-dur" type="number" min="0" value="${strategyObj.ring?.duration_secs ?? 3}"></label>
      <label>Ringback file (early media)<span class="media-widget" data-media="f-ring-file"></span></label>
    </fieldset>
    <fieldset><legend>Reject</legend>
      <label class="inline"><input type="checkbox" id="f-reject-on" ${strategyObj.reject ? "checked" : ""}> Enable reject (overrides answer)</label>
      <label>Response code
        <select id="f-reject-code">
          ${[486, 603, 480, 404].map(c => `<option ${strategyObj.reject?.code === c ? "selected" : ""}>${c}</option>`).join("")}
        </select>
      </label>
      <label>Tone (played via 183 before reject)<span class="media-widget" data-media="f-reject-tone"></span></label>
      <label>Max tone duration (secs)<input id="f-reject-delay" type="number" min="0" value="${strategyObj.reject?.delay_secs ?? 2}"></label>
    </fieldset>
    <fieldset><legend>Answer</legend>
      <label>Action
        <select id="f-answer-action">
          <option value="" ${!strategyObj.answer ? "selected" : ""}>default (play built-in)</option>
          <option value="echo" ${strategyObj.answer?.action === "echo" ? "selected" : ""}>echo</option>
          <option value="play" ${strategyObj.answer?.action === "play" ? "selected" : ""}>play file</option>
        </select>
      </label>
      <label>Play file<span class="media-widget" data-media="f-answer-wav"></span></label>
      <label>SDP jump (200 OK differs from 183: new SSRC/codec/ts/seq)
        <select id="f-sdp-jump">
          <option value="" ${!strategyObj.sdp_jump ? "selected" : ""}>Off</option>
          <option value="1" ${strategyObj.sdp_jump ? "selected" : ""}>On</option>
        </select>
      </label>
      <label>Jump codecs<input id="f-jump-codecs" placeholder="pcmu,g722" value="${esc((strategyObj.jump_codecs || []).join(","))}"></label>
      <label>DTMF flows (scheduled after answer)<input id="f-dtmf-flows" placeholder="1s:2,2s:#" value="${esc(strategyObj.dtmf_flows || "")}"></label>
    </fieldset>
    <fieldset><legend>Caller Announcement</legend>
      <label class="inline"><input type="checkbox" id="f-announce-on" ${strategyObj.announce ? "checked" : ""}> Enable announcement (played after answer)</label>
      <label>Announcement file<span class="media-widget" data-media="f-announce-file"></span></label>
      <label>Stream jump after announcement (no signaling, new SSRC/seq/ts)
        <select id="f-announce-jump">
          <option value="" ${!strategyObj.announce?.jump_after ? "selected" : ""}>No</option>
          <option value="1" ${strategyObj.announce?.jump_after ? "selected" : ""}>Yes</option>
        </select>
      </label>
      <label>Jump codec (optional)<input id="f-announce-codec" placeholder="g722" value="${esc(strategyObj.announce?.jump_codec || "")}"></label>
    </fieldset>
    <fieldset><legend>Hangup</legend>
      <label>Mode
        <select id="f-hangup-mode">
          <option value="remote" ${strategyObj.hangup?.mode === "remote" ? "selected" : ""}>wait for remote BYE</option>
          <option value="after" ${(strategyObj.hangup?.mode === "after") || (strategyObj.hangup?.after_secs && !strategyObj.hangup?.mode) ? "selected" : ""}>send BYE after N secs</option>
          <option value="playback" ${strategyObj.hangup?.mode === "playback" || (!strategyObj.hangup && "x") ? "selected" : ""}>hang up after playback</option>
        </select>
      </label>
      <label>Hangup after (secs)<input id="f-hangup-secs" type="number" min="1" value="${strategyObj.hangup?.after_secs ?? 30}"></label>
    </fieldset>
  `;
  $$(".media-widget", f).forEach(w => buildMediaWidget(w, mediaFiles));
  preselectMedia(f, "f-ring-file", strategyObj.ring?.ringback);
  preselectMedia(f, "f-reject-tone", strategyObj.reject?.tone);
  preselectMedia(f, "f-answer-wav", strategyObj.answer?.wav_file);
  preselectMedia(f, "f-announce-file", strategyObj.announce?.file);

  $("#strategy-modal").classList.remove("hidden");
  $("#sm-close").onclick = () => $("#strategy-modal").classList.add("hidden");
  $("#sm-save").onclick = () => saveStrategy(name);
}

async function saveStrategy(oldName) {
  const newName = collectValue("f-name").trim() || oldName;
  // rename: adjust bindings
  if (newName !== oldName) {
    for (const a of editConfig.accounts) {
      if (a.strategy === oldName) a.strategy = newName;
    }
  }
  let strategy = editConfig.strategies.find(s => s.name === oldName);
  const isNew = !strategy;
  if (isNew) {
    strategy = { name: newName };
    editConfig.strategies.push(strategy);
  }
  strategy.name = newName;
  strategy.match_caller = collectValue("f-match-caller") || null;
  const codecs = collectValue("f-codecs");
  strategy.codecs = codecs ? codecs.split(",").map(s => s.trim()).filter(Boolean) : null;
  // ring
  const ringMode = collectValue("f-ring-mode");
  const ringFile = mediaValue($("#sm-form"), "f-ring-file");
  if (ringMode === "none") strategy.ring = null;
  else if (ringMode === "180") strategy.ring = { duration_secs: num(collectValue("f-ring-dur"), 3) };
  else if (ringMode === "builtin") strategy.ring = { duration_secs: num(collectValue("f-ring-dur"), 3), ringback: "" };
  else strategy.ring = { duration_secs: num(collectValue("f-ring-dur"), 3), ringback: ringFile || "wavs/ringing.wav" };
  // reject
  if ($("#f-reject-on").checked) {
    strategy.reject = {
      code: parseInt(collectValue("f-reject-code")) || 486,
      tone: mediaValue($("#sm-form"), "f-reject-tone") || null,
      delay_secs: num(collectValue("f-reject-delay"), 2),
    };
  } else strategy.reject = null;
  // answer
  const action = collectValue("f-answer-action");
  const wav = mediaValue($("#sm-form"), "f-answer-wav");
  if (action === "echo") strategy.answer = { action: "echo" };
  else if (action === "play") strategy.answer = { action: "play", wav_file: wav || "wavs/play.wav" };
  else strategy.answer = null;
  // sdp jump
  strategy.sdp_jump = collectValue("f-sdp-jump") === "1" ? true : null;
  const jc = collectValue("f-jump-codecs");
  strategy.jump_codecs = jc ? jc.split(",").map(s => s.trim()).filter(Boolean) : null;
  strategy.dtmf_flows = collectValue("f-dtmf-flows") || null;
  // announce
  if ($("#f-announce-on").checked) {
    strategy.announce = {
      file: mediaValue($("#sm-form"), "f-announce-file") || "wavs/announce.wav",
      jump_after: collectValue("f-announce-jump") === "1",
      jump_codec: collectValue("f-announce-codec") || null,
    };
  } else strategy.announce = null;
  // hangup
  const hmode = collectValue("f-hangup-mode");
  if (hmode === "playback") strategy.hangup = null;
  else if (hmode === "remote") strategy.hangup = { mode: "remote" };
  else strategy.hangup = { mode: "after", after_secs: num(collectValue("f-hangup-secs"), 30) };

  const ok = await putConfig(editConfig);
  if (ok) {
    $("#strategy-modal").classList.add("hidden");
    refreshStrategies();
  }
}

async function putConfig(config) {
  try {
    const res = await fetch("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(config),
    });
    if (!res.ok) throw new Error(await res.text());
    return true;
  } catch (e) {
    alert("save failed: " + e);
    return false;
  }
}

// New strategy from template
function initStrategyTemplates() {
  $$("#tpl-bar .tpl").forEach(btn => {
    btn.addEventListener("click", async () => {
      const tpl = STRATEGY_TEMPLATES[btn.dataset.tpl];
      if (!tpl) return;
      const config = await api("/api/config");
      let name = tpl.label;
      let i = 2;
      while (config.strategies.some(s => s.name === name)) name = `${tpl.label}${i++}`;
      const strategy = { name, ...tpl.build() };
      config.strategies.push(strategy);
      if (await putConfig(config)) {
        refreshStrategies();
        // reopen editor for the new strategy once list refreshes
        setTimeout(() => openStrategyModal(name, strategy), 300);
      }
    });
  });
}

// ── 媒体文件三件套: 下拉 + 试听 + 上传覆盖 ──
async function buildMediaWidget(el, mediaFiles) {
  if (!mediaFiles) {
    try { mediaFiles = (await api("/api/media")).media.map(m => m.name); }
    catch (e) { mediaFiles = []; }
  }
  el.innerHTML = `
    <select class="mw-select">
      <option value="">(none)</option>
      ${mediaFiles.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join("")}
    </select>
    <button class="btn mw-play" type="button" title="Preview">▶</button>
    <input type="file" class="mw-file" accept=".wav" style="display:none">
    <button class="btn mw-upload" type="button" title="Upload / overwrite">Upload</button>
  `;
  const select = $(".mw-select", el);
  let audio = null;
  $(".mw-play", el).addEventListener("click", () => {
    if (!select.value) return;
    if (audio) audio.pause();
    audio = new Audio(`/media/${encodeURIComponent(select.value)}`);
    audio.play();
  });
  const fileInput = $(".mw-file", el);
  $(".mw-upload", el).addEventListener("click", () => fileInput.click());
  fileInput.addEventListener("change", async () => {
    const file = fileInput.files[0];
    if (!file) return;
    const name = file.name;
    const fd = new FormData();
    fd.append("file", file);
    const res = await fetch(`/api/media/${encodeURIComponent(name)}`, { method: "POST", body: fd });
    if (res.ok) {
      if (![...select.options].some(o => o.value === name)) {
        select.add(new Option(name, name));
      }
      select.value = name;
    } else {
      alert("upload failed: " + await res.text());
    }
  });
}

function preselectMedia(container, id, value) {
  if (!value) return;
  const w = $(`.media-widget[data-media="${id}"] .mw-select`, container);
  if (w && [...w.options].some(o => o.value === value)) w.value = value;
}
function mediaValue(container, id) {
  const w = $(`.media-widget[data-media="${id}"] .mw-select`, container);
  return w ? w.value : "";
}

// ── 外呼 ──
async function initOutbound() {
  await buildMediaWidget($('[data-media="ob-wav"]'));
  $("#ob-submit").addEventListener("click", async () => {
    const body = {
      target: $("#ob-target").value.trim(),
      from_user: $("#ob-from").value.trim() || "caller",
      action: $("#ob-action").value,
      wav_file: mediaValue($("#outbound-form"), "ob-wav") || null,
      hangup_secs: num($("#ob-hangup").value, 10),
      dtmf_flows: $("#ob-dtmf").value.trim() || null,
      total: num($("#ob-total").value, 1),
      cps: num($("#ob-cps").value, 1),
    };
    if (!body.target) { $("#ob-result").textContent = "target URI is required"; return; }
    try {
      const res = await api("/api/calls", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      $("#ob-result").textContent = `started: ${res.from} → ${res.target}`;
      $$(".tab")[1].click(); // Calls
    } catch (e) {
      $("#ob-result").textContent = "failed: " + e;
    }
  });
}

// --- Calls tab ---
let selectedCallId = null;
let wsConnected = false;

function filterParams() {
  const p = new URLSearchParams();
  const st = $("#f-state")?.value || "";
  const acc = $("#f-account")?.value || "";
  const strat = $("#f-strategy")?.value || "";
  if (st) p.set("state", st);
  if (acc) p.set("account", acc);
  if (strat) p.set("strategy", strat);
  const q = p.toString();
  return q ? `?${q}` : "";
}

function refreshFilterOptions(accounts, strategies) {
  const accSel = $("#f-account");
  const stratSel = $("#f-strategy");
  if (accSel) {
    const cur = accSel.value;
    accSel.innerHTML = `<option value="">All accounts</option>` +
      (accounts || []).map(a => `<option value="${esc(a)}">${esc(a)}</option>`).join("");
    if ([...accSel.options].some(o => o.value === cur)) accSel.value = cur;
  }
  if (stratSel) {
    const cur = stratSel.value;
    stratSel.innerHTML = `<option value="">All strategies</option>` +
      (strategies || []).map(s => `<option value="${esc(s)}">${esc(s)}</option>`).join("");
    if ([...stratSel.options].some(o => o.value === cur)) stratSel.value = cur;
  }
}

async function loadCalls() {
  try {
    const data = await api("/api/calls" + filterParams());
    $("#call-count").textContent = `${data.active || 0} active`;
    const tbody = $("#call-table tbody");
    tbody.innerHTML = "";
    for (const c of (data.calls || [])) {
      const tr = document.createElement("tr");
      if (c.call_id === selectedCallId) tr.classList.add("selected");
      tr.innerHTML = `
        <td>${esc(fmtTime(c))}</td>
        <td>${c.direction === "outbound" ? "↗" : "↙"}</td>
        <td>${esc(shortUser(c.caller))}</td>
        <td>${esc(shortUser(c.callee))}</td>
        <td><span class="state-pill ${c.state || ""}">${stateLabel(c.state)}</span></td>
        <td>${fmtDur(c.duration_ms)}</td>
      `;
      tr.addEventListener("click", () => selectCall(c.call_id));
      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error("loadCalls", e);
  }
}

async function selectCall(callId) {
  selectedCallId = callId;
  loadCalls();
  try {
    const detail = await api(`/api/calls/${encodeURIComponent(callId)}`);
    renderCallDetail(detail);
  } catch (e) {
    $("#call-detail").innerHTML = `<div class="placeholder">failed to load: ${esc(String(e))}</div>`;
  }
}

function renderCallDetail(d) {
  const el = $("#call-detail");
  const active = ["answered", "early_media", "ringing", "trying"].includes(d.state);
  el.innerHTML = `
    <div class="toolbar">
      <h2>${esc(shortUser(d.caller))} → ${esc(shortUser(d.callee))}</h2>
      <span class="state-pill ${d.state || ""}">${stateLabel(d.state)}</span>
      <span class="badge">${esc(d.codec || "-")}</span>
      <span class="badge">${fmtDur(d.duration_ms)}</span>
      <span class="muted mono">${esc(d.call_id)}</span>
      <span style="flex:1"></span>
      ${active ? `<button id="btn-hangup" class="btn danger">Hang up</button>` : ""}
    </div>
    ${active ? `
    <div class="dtmf-bar">
      ${"123456789*0#".split("").map(k => `<button class="btn dtmf" data-d="${k}">${k}</button>`).join("")}
    </div>` : ""}
    ${d.jump_events && d.jump_events.length ? `
    <div class="panel">
      <h3>Media stream jumps (${d.jump_events.length})</h3>
      ${d.jump_events.map(j => `
        <div class="jump-event">
          <span class="mono">+${(j.t_ms / 1000).toFixed(1)}s</span>
          <span class="badge warn">${esc(j.reason)}</span>
          SSRC <span class="mono old">${j.old_ssrc ?? "-"}</span> →
          <span class="mono new">${j.new_ssrc ?? "-"}</span>
        </div>`).join("")}
    </div>` : ""}
    <div class="panel">
      <h3>SIP Signaling</h3>
      <div class="sig-wrap">
        ${renderTrace(d.sip_trace)}
      </div>
    </div>
    <div class="panel">
      <h3>RTP / RTCP</h3>
      ${renderStats(d.stats)}
    </div>
    ${d.dtmf_events && d.dtmf_events.length ? `
    <div class="panel">
      <h3>DTMF Events</h3>
      ${d.dtmf_events.map(e => `
        <span class="badge ${e.dir === "rx" ? "info" : "ok"}" title="${e.dir} @${(e.t_ms/1000).toFixed(1)}s">${e.dir === "rx" ? "rx" : "tx"} ${esc(e.digit)}</span>`).join(" ")}
    </div>` : ""}
    <div class="panel">
      <h3>SDP Comparison</h3>
      ${renderSdpDiff(d)}
    </div>
    <div class="panel">
      <h3>Recording</h3>
      ${d.recording
        ? `<audio controls src="/recordings/${encodeURIComponent(recName(d.recording))}"></audio>
           <div class="muted mono">${esc(recName(d.recording))}</div>`
        : `<div class="muted">no recording (configure the recorders directory to enable)</div>`}
    </div>
  `;
  bindTraceActions(el, d.sip_trace || []);
  const btn = $("#btn-hangup");
  if (btn) btn.addEventListener("click", async () => {
    await api(`/api/calls/${encodeURIComponent(d.call_id)}/hangup`, { method: "POST" });
  });
  $$(".btn.dtmf", el).forEach(b => b.addEventListener("click", async () => {
    await api(`/api/calls/${encodeURIComponent(d.call_id)}/dtmf`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ digit: b.dataset.d }),
    });
  }));
  // auto-refresh stats while active
  if (active) {
    clearTimeout(renderCallDetail._t);
    renderCallDetail._t = setTimeout(() => {
      if (selectedCallId === d.call_id) selectCall(d.call_id);
    }, 1500);
  }
}

function renderTrace(trace) {
  if (!trace || !trace.length) return `<div class="muted">none</div>`;
  return `
    <div class="sig-split">
      <div class="sig-list">
        ${trace.map((e, i) => `
          <div class="sig-row ${e.dir}" data-idx="${i}" title="${esc(e.peer || "")}">
            <span class="mono t">+${(e.t_ms / 1000).toFixed(3)}s</span>
            <span class="arrow">${e.dir === "in" ? "←" : "→"}</span>
            <span class="summary ${e.dir}">${esc(e.summary)}</span>
          </div>`).join("")}
      </div>
      <div class="sig-detail">
        <pre class="raw open" id="sig-raw">select a message to view its raw content</pre>
      </div>
    </div>`;
}

function bindTraceActions(root, trace) {
  const list = $(".sig-list", root);
  if (!list) return;
  const show = (idx) => {
    const e = trace[idx];
    const pre = $("#sig-raw", root);
    if (e && pre) pre.textContent = e.raw || "(raw not available for live-pushed events)";
  };
  list.addEventListener("click", (ev) => {
    const row = ev.target.closest(".sig-row");
    if (!row) return;
    $$(".sig-row", list).forEach(r => r.classList.toggle("selected", r === row));
    show(+row.dataset.idx);
  });
  // auto-select the first message
  const first = $(".sig-row", list);
  if (first) {
    first.classList.add("selected");
    show(0);
  }
}

function renderStats(st) {
  if (!st) return `<div class="muted">no stats available</div>`;
  const rows = [
    ["Sent RTP", `${st.tx_packets} pkts / ${fmtBytes(st.tx_bytes)}`],
    ["Received RTP", `${st.rx_packets} pkts / ${fmtBytes(st.rx_bytes)}`],
    ["Lost", `${st.rx_lost} (${st.loss_rate?.toFixed(2) ?? 0}%)`],
    ["RTCP RTT", `${(st.rtcp_rtt_ms ?? 0).toFixed(1)} ms (${st.rtcp_rtt_samples} samples)`],
    ["NACK", `${st.nack_sent} sent / ${st.nack_recv} recv / ${st.nack_recovered} recovered`],
    ["Seq gaps", `${st.seq_gap_events} events / ${st.seq_gap_total} pkts / max ${st.seq_gap_max}`],
    ["Reordered", st.seq_reorder_events],
    ["TS jumps", `${st.ts_jump_events} events / max ${st.ts_jump_ms_max}ms`],
    ["Stream switches", st.stream_switch_events],
    ["DTMF", `${st.rx_dtmf_events} rx / ${st.tx_dtmf_events} tx`],
    ["Setup latency", `${(st.setup_latency_ms ?? 0).toFixed(0)} ms`],
  ];
  return `<table class="stats-table">${rows.map(([k, v]) =>
    `<tr><td>${k}</td><td class="mono">${esc(String(v))}</td></tr>`).join("")}</table>`;
}

function renderSdpDiff(d) {
  const blocks = [
    ["INVITE offer", d.sdp_offer],
    ["183 answer", d.sdp_183],
    ["200 OK answer", d.sdp_200],
  ].filter(([, s]) => s);
  if (!blocks.length) return `<div class="muted">no SDP captured</div>`;
  if (blocks.length >= 2) {
    const [labelA, a] = blocks[blocks.length - 2];
    const [labelB, b] = blocks[blocks.length - 1];
    const da = parseSdp(a), db = parseSdp(b);
    return `
      <div class="muted">compare ${esc(labelA)} vs ${esc(labelB)} <b>${da.ssrc !== db.ssrc || da.port !== db.port || da.codec !== db.codec ? "(JUMPED)" : ""}</b></div>
      <table class="sdp-diff">
        <tr><th></th><th>${esc(labelA)}</th><th>${esc(labelB)}</th><th></th></tr>
        ${[["SSRC", da.ssrc, db.ssrc], ["Codec", da.codec, db.codec], ["Port", da.port, db.port], ["Addr", da.addr, db.addr], ["o= version", da.overs, db.overs]]
          .map(([k, x, y]) => `
          <tr class="${x !== y ? "changed" : ""}">
            <td>${k}</td><td class="mono">${esc(String(x ?? "-"))}</td><td class="mono">${esc(String(y ?? "-"))}</td>
            <td>${x !== y ? "≠" : "="}</td>
          </tr>`).join("")}
      </table>
      <details><summary>Raw SDP</summary>
        ${blocks.map(([label, s]) => `<div class="muted">${esc(label)}</div><pre class="raw">${esc(s)}</pre>`).join("")}
      </details>`;
  }
  const [label, s] = blocks[0];
  return `<div class="muted">${esc(label)}</div><pre class="raw">${esc(s)}</pre>`;
}

function parseSdp(sdp) {
  const get = (re) => (sdp.match(re) || [])[1] || null;
  const o = get(/o=\S+ (\S+) (\S+)/);
  return {
    ssrc: get(/a=ssrc:\s*(\d+)/) || get(/y=(\d+)/),
    codec: get(/a=rtpmap:\d+ ([^/ ]+)/)?.toUpperCase(),
    port: get(/m=audio (\d+)/),
    addr: get(/c=IN IP4 (\S+)/),
    overs: o ? o.split(" ")[0] : null,
  };
}

function recName(path) {
  return String(path || "").split("/").pop();
}

// ── WebSocket 实时推送 ──
function connectWs() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen = () => {
    wsConnected = true;
    $("#conn-badge .dot").classList.replace("off", "on");
    $("#conn-text").textContent = "connected";
  };
  ws.onclose = () => {
    wsConnected = false;
    $("#conn-badge .dot").classList.replace("on", "off");
    $("#conn-text").textContent = "offline";
    setTimeout(connectWs, 2000);
  };
  ws.onmessage = (ev) => {
    try {
      const msg = JSON.parse(ev.data);
      if (msg.type === "call_state" || msg.type === "call_ended") {
        if (msg.call_id === selectedCallId) selectCall(msg.call_id);
        else loadCalls();
      }
    } catch (e) { /* ignore */ }
  };
}

// --- utils ---
function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
function shortUser(uri) {
  const m = String(uri || "").match(/sip:([^@;]+)@?([^;>]*)/);
  return m ? m[1] : String(uri || "-");
}
function stateLabel(s) {
  return { trying: "TRYING", ringing: "RINGING", early_media: "EARLY-MEDIA", answered: "IN CALL",
           terminated: "ENDED", rejected: "REJECTED", failed: "FAILED" }[s] || s || "-";
}
function fmtDur(ms) {
  if (ms == null) return "-";
  const s = Math.floor(ms / 1000);
  return s >= 60 ? `${Math.floor(s / 60)}m${s % 60}s` : `${s}s`;
}
function fmtBytes(b) {
  if (b == null) return "-";
  if (b > 1e6) return `${(b / 1e6).toFixed(1)} MB`;
  if (b > 1e3) return `${(b / 1e3).toFixed(1)} KB`;
  return `${b} B`;
}
function fmtTime(c) {
  if (!c.started_at_ms) return "-";
  const d = new Date(c.started_at_ms);
  const p = n => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ` +
         `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

// ── boot ──
loadAccounts();
loadStrategies();
loadCalls();
for (const id of ["f-state", "f-account", "f-strategy"]) {
  document.getElementById(id)?.addEventListener("change", loadCalls);
}
initOutbound();
initStrategyTemplates();
initAddAccount();
connectWs();
setInterval(loadCalls, 2000);
setInterval(() => { loadAccounts(); loadStrategies(); }, 5000);
