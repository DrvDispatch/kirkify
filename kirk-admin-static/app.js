// /srv/kirk-admin-static/app.js
/**
 * Kirkify Admin SPA
 * - FastAPI backend @ https://api.keyauth.eu
 */

const API_BASE_URL = "https://api.keyauth.eu";

// ---------- helpers ----------
function $(sel) { return document.querySelector(sel); }
function $all(sel) { return Array.from(document.querySelectorAll(sel)); }

function getToken() { return localStorage.getItem("kirk_admin_jwt") || null; }
function setToken(t) { localStorage.setItem("kirk_admin_jwt", t); }
function clearToken() { localStorage.removeItem("kirk_admin_jwt"); }

async function api(path, options = {}) {
  const token = getToken();
  const headers = options.headers || {};
  if (!options.noAuth) {
    if (!token) throw new Error("Not authenticated");
    headers["Authorization"] = `Bearer ${token}`;
  }
  if (options.body && !(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(options.body);
  }
  const res = await fetch(API_BASE_URL + path, { ...options, headers });
  if (!res.ok) {
    let detail = "";
    try { const j = await res.json(); detail = j.detail || j.error || res.statusText; }
    catch { detail = res.statusText; }
    throw new Error(`HTTP ${res.status}: ${detail}`);
  }
  const ct = res.headers.get("Content-Type") || "";
  return ct.includes("application/json") ? res.json() : res.text();
}

// ---------- view switching ----------
function showView(name) {
  $all(".view").forEach(v => v.classList.remove("view--active"));
  const el = $(`#view-${name}`); if (el) el.classList.add("view--active");
}
function showNav(name) {
  $all(".nav-section").forEach(s => s.classList.remove("nav-section--active"));
  $all(".nav-btn").forEach(b => b.classList.remove("is-active"));
  const sec = $(`#${name}`); const btn = $(`.nav-btn[data-nav="${name}"]`);
  if (sec) sec.classList.add("nav-section--active");
  if (btn) btn.classList.add("is-active");
}

// ---------- auth ----------
async function attemptAutoLogin() {
  const token = getToken();
  if (!token) { showView("login"); return; }
  try {
    const me = await api("/api/auth/me");
    $("#topbar-user").textContent = me.user;
    showView("dashboard");
    showNav("nav-overview");
    await loadOverview();
    await loadGpuOverview();
  } catch (e) {
    clearToken(); showView("login");
  }
}
async function handleLoginSubmit(e) {
  e.preventDefault();
  const user = $("#login-username").value.trim();
  const pass = $("#login-password").value;
  const err = $("#login-error");
  err.hidden = true; err.textContent = "";
  try {
    const res = await api("/api/auth/login", { method: "POST", body: { username: user, password: pass }, noAuth: true });
    setToken(res.token);
    $("#topbar-user").textContent = res.user;
    $("#login-password").value = "";
    showView("dashboard"); showNav("nav-overview");
    await loadOverview(); await loadGpuOverview();
  } catch (ex) { err.hidden = false; err.textContent = ex.message; }
}
function handleLogout() { clearToken(); window.location.reload(); }

// ---------- overview ----------
async function loadOverview() {
  try {
    const m = await api("/api/metrics");
    $("#overview-total-jobs").textContent = m.total_sampled ?? "0";
    $("#overview-avg-ms").textContent = m.avg_processing_ms ? `${Math.round(m.avg_processing_ms)} ms` : "—";
    const container = $("#overview-status-table");
    const by = m.by_status || {};
    if (!Object.keys(by).length) { container.innerHTML = "<div class='table table--compact'><div style='padding:8px'>No jobs in sample.</div></div>"; return; }
    let html = "<table><thead><tr><th>Status</th><th>Count</th></tr></thead><tbody>";
    for (const [status, count] of Object.entries(by)) html += `<tr><td>${status}</td><td>${count}</td></tr>`;
    html += "</tbody></table>";
    container.innerHTML = `<div class="table table--compact">${html}</div>`;
  } catch (e) { console.error("overview metrics error:", e); }
}
async function loadGpuOverview() {
  try {
    const s = await fetch(API_BASE_URL + "/api/gpu_status").then(r => r.json());
    $("#overview-gpu-status").textContent = s.status || "off";
    $("#overview-gpu-extra").textContent = `actual=${s.actual} worker_ok=${s.worker_ok} jobs=${s.active_jobs}`;
  } catch (e) {
    $("#overview-gpu-status").textContent = "error"; $("#overview-gpu-extra").textContent = String(e);
  }
}

// ---------- jobs ----------
let currentJobSSE = null;
let jobsOffset = 0;

function statusBadgeClass(status) {
  if (!status) return "badge";
  const s = status.toLowerCase();
  if (["completed"].includes(s)) return "badge badge--status-completed";
  if (["failed","timeout","worker_failed","worker_unreachable","startup_timeout"].includes(s)) return "badge badge--status-failed";
  if (["processing","starting"].includes(s)) return "badge badge--status-processing";
  if (["queued"].includes(s)) return "badge badge--status-queued";
  if (["canceled"].includes(s)) return "badge";
  return "badge";
}

async function loadJobs() {
  const status = $("#jobs-filter-status").value || "";
  const q = ($("#jobs-filter-q").value || "").trim();
  const limit = Math.max(5, Math.min(200, parseInt($("#jobs-limit").value || "50", 10)));
  const qs = new URLSearchParams();
  if (status) qs.set("status", status);
  if (q) qs.set("q", q);
  qs.set("limit", String(limit));
  qs.set("offset", String(jobsOffset));
  try {
    const res = await api(`/api/jobs?${qs.toString()}`);
    const items = res.items || [];
    let html = "<table><thead><tr>" +
      "<th>ID</th><th>Status</th><th>Filename</th><th>IP</th><th>Created</th><th></th>" +
      "</tr></thead><tbody>";
    for (const j of items) {
      const created = j.created_at ? new Date(j.created_at).toLocaleString() : "—";
      html += `<tr>
        <td><span class="mono">${j.id}</span></td>
        <td><span class="${statusBadgeClass(j.status)}">${j.status}</span></td>
        <td>${j.filename || ""}</td>
        <td>${j.requested_by_ip || ""}</td>
        <td>${created}</td>
        <td>
          <button class="btn btn--sm" data-job-open="${j.id}">Open</button>
          <button class="btn btn--sm btn--danger" data-job-delete="${j.id}">Delete</button>
        </td>
      </tr>`;
    }
    html += "</tbody></table>";
    $("#jobs-table").innerHTML = `<div class="table">${html}</div>`;
  } catch (e) {
    console.error("loadJobs error:", e);
    $("#jobs-table").innerHTML = `<div class="table"><div style="padding:8px">Error: ${e.message}</div></div>`;
  }
}

async function openJobDetail(jobId) {
  if (!jobId) return;
  if (currentJobSSE) { currentJobSSE.close(); currentJobSSE = null; }
  $("#job-detail").hidden = false;
  $("#job-detail-title").textContent = `Job ${jobId}`;
  $("#job-events").innerHTML = "";
  $("#job-input-img").innerHTML = "Loading…";
  $("#job-output-img").innerHTML = "No output yet";

  try {
    const resp = await api(`/api/jobs/${jobId}`);
    const job = resp.job;
    const events = resp.events || [];

    $("#job-detail-status").className = statusBadgeClass(job.status);
    $("#job-detail-status").textContent = job.status;

    const infoDl = $("#job-detail-info"); infoDl.innerHTML = "";
    function addInfo(label, value) {
      const dt = document.createElement("dt"); dt.textContent = label;
      const dd = document.createElement("dd"); dd.textContent = value ?? "—";
      infoDl.appendChild(dt); infoDl.appendChild(dd);
    }
    addInfo("ID", job.id);
    addInfo("Status", job.status);
    addInfo("IP", job.requested_by_ip);
    addInfo("Vast instance", job.vast_instance_id);
    addInfo("Filename", job.filename);
    addInfo("Mode", job.mode);
    addInfo("Created", job.created_at ? new Date(job.created_at).toLocaleString() : "—");
    addInfo("Started", job.started_at ? new Date(job.started_at).toLocaleString() : "—");
    addInfo("Finished", job.finished_at ? new Date(job.finished_at).toLocaleString() : "—");
    addInfo("Processing time", job.processing_ms ? `${Math.round(job.processing_ms/1000)} s` : "—");
    addInfo("Error", job.error || "");

    // Render existing events
    for (const e of events) appendJobEvent(e);

    // Load input image right away
    await loadJobImage(jobId, "input", "#job-input-img");

    // Load output only when available
    if (job.status === "completed") await loadJobImage(jobId, "output", "#job-output-img");
    else $("#job-output-img").textContent = "—";

    // Bind actions
    $("#job-cancel").onclick = async () => {
      try { await api(`/api/jobs/${jobId}/cancel`, { method: "POST" }); await openJobDetail(jobId); }
      catch (e) { alert(e.message); }
    };
    $("#job-retry").onclick = async () => {
      try { const r = await api(`/api/jobs/${jobId}/retry`, { method: "POST" }); alert(`Retry queued: ${r.new_job_id}`); }
      catch (e) { alert(e.message); }
    };
    $("#job-delete").onclick = async () => {
      try { await api(`/api/jobs/${jobId}`, { method: "DELETE" }); $("#job-detail").hidden = true; await loadJobs(); }
      catch (e) { alert(e.message); }
    };

    // SSE stream
    const token = getToken();
    if (token) {
      const url = `${API_BASE_URL}/api/jobs/${jobId}/events/stream?token=${encodeURIComponent(token)}`;
      const es = new EventSource(url);
      currentJobSSE = es;
      es.onmessage = (ev) => {
        try { const data = JSON.parse(ev.data); appendJobEvent(data); }
        catch (err) { console.warn("SSE parse error:", err); }
      };
      es.onerror = () => { es.close(); currentJobSSE = null; };
    }
  } catch (e) { console.error("openJobDetail error:", e); }
}

function appendJobEvent(e) {
  const container = $("#job-events");
  const div = document.createElement("div");
  const cls = (e.type || "").toLowerCase();
  div.className = "job-events__item " + (cls.includes("error") ? "is-error" : cls.includes("done") ? "is-ok" : "");
  const ts = e.ts ? new Date(e.ts).toLocaleTimeString() : "";
  const msg = e.message || "";
  const type = e.type || "";
  const prog = e.progress != null ? ` (${e.progress}%)` : "";
  div.textContent = `[${ts}] [${type}] ${msg}${prog}`;
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
}

async function loadJobImage(jobId, kind, targetSel) {
  const target = $(targetSel);
  try {
    const res = await api(`/api/jobs/${jobId}/signed_url?kind=${kind}`);
    const img = document.createElement("img");
    img.src = res.url; img.alt = `${kind} image`;
    target.innerHTML = ""; target.appendChild(img);
  } catch { target.textContent = "—"; }
}

// ---------- logs ----------
async function loadWorkerLogs() {
  try {
    const { items } = await api("/api/worker_logs");
    let html = "<table><thead><tr><th>Time</th><th>Level</th><th>Message</th></tr></thead><tbody>";
    for (const row of items) {
      html += `<tr><td>${row.ts ? new Date(row.ts).toLocaleString() : "—"}</td><td>${row.level || ""}</td><td>${row.message || ""}</td></tr>`;
    }
    html += "</tbody></table>";
    $("#logs-table").innerHTML = html;
  } catch (e) {
    $("#logs-table").innerHTML = `<div style="padding:8px">Error: ${e.message}</div>`;
  }
}

// ---------- settings ----------
async function loadSettingsGpu() {
  try {
    const status = await fetch(API_BASE_URL + "/api/gpu_status").then(r => r.json());
    $("#settings-gpu-json").textContent = JSON.stringify(status, null, 2);
  } catch (e) {
    $("#settings-gpu-json").textContent = `Error: ${e.message}`;
  }
}
async function warmGpu() {
  try { const res = await api("/api/warm_gpu", { method: "POST" }); alert(res.message || "Warm requested"); await loadSettingsGpu(); }
  catch (e) { alert("Error: " + e.message); }
}
async function manualStartGpu() {
  try { const res = await api("/api/manual_start", { method: "POST" }); alert(res.message || "Start requested"); await loadSettingsGpu(); }
  catch (e) { alert("Error starting: " + e.message); }
}
async function manualStopGpu() {
  try { const res = await api("/api/manual_stop", { method: "POST" }); alert(res.message || "Stop requested"); await loadSettingsGpu(); }
  catch (e) { alert("Error stopping: " + e.message); }
}

// ---------- wiring ----------
document.addEventListener("DOMContentLoaded", () => {
  // login
  $("#login-form").addEventListener("submit", handleLoginSubmit);
  $("#logout-btn").addEventListener("click", handleLogout);

  // nav
  $all(".nav-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const name = btn.getAttribute("data-nav");
      if (name) {
        showNav(name);
        if (name === "nav-overview") { loadOverview(); loadGpuOverview(); }
        else if (name === "nav-jobs") { loadJobs(); }
        else if (name === "nav-logs") { loadWorkerLogs(); }
        else if (name === "nav-settings") { loadSettingsGpu(); }
      }
    });
  });

  // overview
  $("#overview-refresh").addEventListener("click", () => { loadOverview(); loadGpuOverview(); });

  // jobs
  $("#jobs-refresh").addEventListener("click", () => { jobsOffset = 0; loadJobs(); });
  $("#jobs-filter-status").addEventListener("change", () => { jobsOffset = 0; loadJobs(); });
  $("#jobs-filter-q").addEventListener("input", () => { jobsOffset = 0; }); // debounce not needed
  $("#jobs-limit").addEventListener("change", () => { jobsOffset = 0; loadJobs(); });
  $("#jobs-prev").addEventListener("click", () => { jobsOffset = Math.max(0, jobsOffset - Math.max(5, parseInt($("#jobs-limit").value || "50", 10))); loadJobs(); });
  $("#jobs-next").addEventListener("click", () => { jobsOffset += Math.max(5, parseInt($("#jobs-limit").value || "50", 10)); loadJobs(); });

  $("#jobs-table").addEventListener("click", (e) => {
    const openBtn = e.target.closest("[data-job-open]");
    const delBtn = e.target.closest("[data-job-delete]");
    if (openBtn) { openJobDetail(openBtn.getAttribute("data-job-open")); }
    if (delBtn) {
      const id = delBtn.getAttribute("data-job-delete");
      if (confirm(`Delete job ${id}?`)) api(`/api/jobs/${id}`, { method: "DELETE" }).then(loadJobs).catch(err => alert(err.message));
    }
  });

  $("#job-detail-close").addEventListener("click", () => {
    $("#job-detail").hidden = true;
    if (currentJobSSE) { currentJobSSE.close(); currentJobSSE = null; }
  });

  // logs
  $("#logs-refresh").addEventListener("click", loadWorkerLogs);

  // settings
  $("#settings-refresh").addEventListener("click", loadSettingsGpu);
  $("#settings-warm").addEventListener("click", warmGpu);
  $("#settings-start").addEventListener("click", manualStartGpu);
  $("#settings-stop").addEventListener("click", manualStopGpu);

  // boot
  attemptAutoLogin();
});
