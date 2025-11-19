/* assets/js/main.js */
(() => {
  "use strict";

  // ====== KirkApp small utilities ============================================================
  const KirkApp = {
    CONTROLLER_URL: "https://api.keyauth.eu",
    AVG_JOB_SEC_FALLBACK: 75, // used if controller cannot compute a recent average
    MAX_JOBS_IN_UI: 12,
    log(...args) { const ts = new Date().toISOString(); console.log(`[KirkApp ${ts}]`, ...args); },
    $: (sel, root = document) => root.querySelector(sel),
    $$: (sel, root = document) => Array.from(root.querySelectorAll(sel)),
    sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
    on(el, ev, fn, opts) { if (el) el.addEventListener(ev, fn, opts); },
    fmtTime(tsIso) {
      try { const d = new Date(tsIso); return d.toLocaleString(); } catch { return ""; }
    },
    fmtDuration(sec) {
      if (!isFinite(sec) || sec <= 0) return "a moment";
      const m = Math.floor(sec / 60);
      const s = Math.round(sec % 60);
      if (m >= 60) {
        const h = Math.floor(m / 60);
        const rm = m % 60;
        return rm ? `${h}h ${rm}m` : `${h}h`;
      }
      return m ? `${m}m ${s}s` : `${s}s`;
    },
  };

  // ====== persistent client id (localStorage + cookie) =======================================
  const CLIENT_ID_KEY = "kirk_cid";
  function setCookie(name, value, maxAgeDays = 365) {
    const maxAge = maxAgeDays * 24 * 60 * 60;
    const secure = location.protocol === "https:" ? "; Secure" : "";
    document.cookie = `${encodeURIComponent(name)}=${encodeURIComponent(value)}; Path=/; Max-Age=${maxAge}; SameSite=Lax${secure}`;
  }
  function getCookie(name) {
    const m = document.cookie.match(new RegExp(`(?:^|; )${name}=([^;]*)`));
    return m ? decodeURIComponent(m[1]) : "";
  }
  function ensureClientId() {
    let cid = localStorage.getItem(CLIENT_ID_KEY) || getCookie(CLIENT_ID_KEY);
    if (!cid) {
      cid = (window.crypto && crypto.randomUUID) ? crypto.randomUUID() :
            Math.random().toString(16).slice(2) + Date.now().toString(16);
      localStorage.setItem(CLIENT_ID_KEY, cid);
      setCookie(CLIENT_ID_KEY, cid, 365);
    } else {
      // keep cookie fresh even if only LS had it
      setCookie(CLIENT_ID_KEY, cid, 365);
      localStorage.setItem(CLIENT_ID_KEY, cid);
    }
    return cid;
  }

  // ====== Before/After slider ================================================================
  function initBeforeAfter(root) {
    if (!root) return;
    const range = root.querySelector(".ba__range");
    const setPos = (pct) => {
      const clamped = Math.max(0, Math.min(100, pct));
      root.style.setProperty("--pos", clamped + "%");
      if (range) range.value = clamped;
    };
    const pctFromClientX = (clientX) => {
      const rect = root.getBoundingClientRect();
      return rect.width > 0 ? ((clientX - rect.left) / rect.width) * 100 : 50;
    };
    const move = (e) => { const t = e.touches?.[0] || e; setPos(pctFromClientX(t.clientX)); e.preventDefault?.(); };
    const end = () => {
      window.removeEventListener("mousemove", move);
      window.removeEventListener("touchmove", move);
      window.removeEventListener("mouseup", end);
      window.removeEventListener("touchend", end);
    };
    const start = (e) => {
      const t = e.touches?.[0] || e;
      setPos(pctFromClientX(t.clientX));
      window.addEventListener("mousemove", move);
      window.addEventListener("touchmove", move, { passive: false });
      window.addEventListener("mouseup", end);
      window.addEventListener("touchend", end);
      e.preventDefault?.();
    };
    KirkApp.on(root, "mousedown", start);
    KirkApp.on(root, "touchstart", start, { passive: false });
    if (range) KirkApp.on(range, "input", (e) => setPos(parseFloat(e.target.value || "50") || 50));
    setPos(50);
  }
  function initAllBeforeAfter() { KirkApp.$$(".ba").forEach(initBeforeAfter); }

  // ====== HUD / Steps progress ================================================================
  const HUD = (() => {
    const hud = KirkApp.$("#hud");
    const bar = KirkApp.$(".hud__bar");
    const pb = KirkApp.$(".hud__progress");
    const live = KirkApp.$("#hud-live");
    const steps = KirkApp.$$(".hud__steps li");
    const queueEl = KirkApp.$("#hud-queue");
    const etaEl = KirkApp.$("#hud-eta");
    const comeBackEl = KirkApp.$("#hud-return");
    const liFor = (step) => document.querySelector(`.hud__steps [data-step="${step}"]`);

    function show() {
      if (!hud) return;
      hud.hidden = false; hud.classList.add("is-open");
      if (pb) pb.setAttribute("aria-valuenow", "0");
      if (comeBackEl) comeBackEl.hidden = false;
    }
    function hide() {
      if (!hud) return;
      hud.classList.remove("is-open");
      hud.classList.add("is-closing");
      setTimeout(() => { hud.hidden = true; hud.classList.remove("is-closing"); }, 220);
      setQueue(null);
      setEta(null);
    }
    function updateHudProgress() {
      if (!steps.length) return;
      const done = steps.filter((li) => li.classList.contains("is-done")).length;
      const total = steps.length;
      const pct = Math.max(2, (done / total) * 100);
      if (bar) bar.style.width = pct + "%";
      if (pb) pb.setAttribute("aria-valuenow", String(done));
      if (live) live.textContent = `Step ${Math.min(done + 1, total)} of ${total}`;
    }
    function mark(step, state) {
      const li = liFor(step); if (!li) return;
      li.classList.remove("is-done", "is-active");
      if (state === "active") li.classList.add("is-active");
      if (state === "done") li.classList.add("is-done");
      updateHudProgress();
    }
    function setQueue(position) {
      if (!queueEl) return;
      if (!position || position <= 1) { queueEl.textContent = ""; queueEl.hidden = true; return; }
      const ahead = position - 1;
      queueEl.hidden = false;
      queueEl.textContent = `Queue: you are #${position} (${ahead} ahead of you)…`;
    }
    function setEta(sec) {
      if (!etaEl) return;
      if (!sec || sec <= 0) { etaEl.textContent = ""; etaEl.hidden = true; return; }
      etaEl.hidden = false;
      etaEl.textContent = `Estimated wait: ~${KirkApp.fmtDuration(sec)}.`;
    }
    return { show, hide, mark, setQueue, setEta };
  })();

  // ====== GPU chip loop + ETA source ==========================================================
  async function gpuStatus() {
    try {
      const res = await fetch(`${KirkApp.CONTROLLER_URL}/api/gpu_status`, { mode: "cors" });
      if (!res.ok) throw new Error("gpu_status failed");
      return await res.json();
    } catch (e) {
      return { status: "off" };
    }
  }
  async function waitTime() {
    try {
      const res = await fetch(`${KirkApp.CONTROLLER_URL}/api/wait_time`, { mode: "cors" });
      if (res.ok) return await res.json();
    } catch {}
    // fallback to chip math
    const s = await gpuStatus();
    const cap = Math.max(1, Number(s.total_capacity || 1));
    const ahead = Number(s.queued_jobs || 0) + Number(s.active_jobs || 0);
    const est = Math.ceil((ahead / cap) * KirkApp.AVG_JOB_SEC_FALLBACK);
    return { ok: true, avg_job_sec: KirkApp.AVG_JOB_SEC_FALLBACK, estimated_sec: est, source: "fallback" };
  }
  async function loopGpuChip() {
    const el = KirkApp.$("#gpu-status");
    try {
      const s = await gpuStatus();
      if (el) el.textContent = s.status || "—";
      el?.parentElement?.classList.toggle("is-offline", s.status !== "ready");
    } catch {
      if (el) el.textContent = "offline";
      el?.parentElement?.classList.add("is-offline");
    }
  }

  // ====== Job client (SSE) + persistent resume ===============================================
  const JobClient = (() => {
    const events = {};
    function on(jobId, handler) { events[jobId] = handler; }
    function openEvents(jobId) {
      const url = `${KirkApp.CONTROLLER_URL}/api/jobs/${jobId}/events`;
      const es = new EventSource(url, { withCredentials: false });
      es.onmessage = (ev) => {
        try { const payload = JSON.parse(ev.data || "{}"); events[jobId]?.(payload); }
        catch (e) { KirkApp.log("SSE parse error", e); }
      };
      es.onerror = () => { /* browser will retry */ };
      return es;
    }
    async function createJob(file) {
      const cid = ensureClientId();
      const form = new FormData();
      form.append("file", file);
      const res = await fetch(`${KirkApp.CONTROLLER_URL}/api/jobs`, {
        method: "POST",
        body: form,
        headers: { "X-Client-Id": cid },
        mode: "cors",
      });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`Job create failed: ${res.status} ${text || ""}`);
      }
      const data = await res.json();
      return { job_id: data.id, status: data.status };
    }
    async function myJobs(params = {}) {
      const cid = ensureClientId();
      const qs = new URLSearchParams({ ...params, limit: String(params.limit || KirkApp.MAX_JOBS_IN_UI) });
      const res = await fetch(`${KirkApp.CONTROLLER_URL}/api/my/jobs?` + qs.toString(), {
        headers: { "X-Client-Id": cid },
        mode: "cors",
      });
      if (!res.ok) return { items: [] };
      return res.json();
    }
    async function mySignedUrl(jobId, kind = "output") {
      const cid = ensureClientId();
      const qs = new URLSearchParams({ job_id: jobId, kind, client_id: cid });
      const res = await fetch(`${KirkApp.CONTROLLER_URL}/api/my/signed_url?` + qs.toString(), { mode: "cors" });
      if (!res.ok) throw new Error("signed_url failed");
      return res.json(); // {ok, url}
    }
    return { createJob, openEvents, on, myJobs, mySignedUrl };
  })();

  // ====== My Jobs UI ==========================================================================
  async function renderJobs() {
    const wrap = KirkApp.$("#jobs-list");
    const hint = KirkApp.$("#jobs-hint");
    if (!wrap) return;
    wrap.innerHTML = `<div class="jobs-empty">Loading your recent jobs…</div>`;
    try {
      const { items } = await JobClient.myJobs();
      if (!items || !items.length) {
        wrap.innerHTML = `<div class="jobs-empty">No recent jobs yet. Upload an image to get started.</div>`;
        hint && (hint.textContent = "");
        return;
      }
      wrap.innerHTML = "";
      hint && (hint.textContent = "Jobs are saved to this browser. You can close the tab and come back later.");
      for (const job of items) {
        wrap.appendChild(jobCard(job));
      }
      // Begin lazy load previews + live watchers
      for (const job of items) {
        loadJobPreviews(job).catch(() => {});
        if (job.status === "queued" || job.status === "processing") {
          const es = JobClient.openEvents(job.id);
          JobClient.on(job.id, (ev) => {
            const card = KirkApp.$(`[data-job="${job.id}"]`);
            if (!card) return;
            updateJobCardState(card, ev);
          });
          // close eventsource after a while to avoid many open conns
          setTimeout(() => es.close(), 15 * 60 * 1000);
        }
      }
    } catch (e) {
      wrap.innerHTML = `<div class="jobs-empty">Failed to load jobs. Please try again.</div>`;
    }
  }

  function jobCard(job) {
    const el = document.createElement("article");
    el.className = "job-card";
    el.dataset.job = job.id;
    el.innerHTML = `
      <div class="job-head">
        <div class="job-title">
          <span class="chip chip--${job.status || "queued"}">${(job.status || "queued").toUpperCase()}</span>
          <span class="job-id">#${job.id.slice(0, 8)}</span>
        </div>
        <div class="job-meta">${job.created_at ? KirkApp.fmtTime(job.created_at) : ""}</div>
      </div>
      <div class="job-grid">
        <div class="job-img">
          <div class="job-img__label">Input</div>
          <img class="job-img__el job-img__input" alt="Input image" />
        </div>
        <div class="job-img">
          <div class="job-img__label">Output</div>
          <img class="job-img__el job-img__output" alt="Output image"/>
        </div>
      </div>
      <div class="job-actions">
        <a class="btn-link btn-open is-disabled" href="#" target="_blank" rel="noopener">Open result</a>
        <a class="btn-link btn-dl is-disabled" href="#" download="kirkified.jpg" aria-disabled="true">Download</a>
      </div>
    `;
    return el;
  }

  async function loadJobPreviews(job) {
    const card = KirkApp.$(`[data-job="${job.id}"]`);
    if (!card) return;
    const inEl = card.querySelector(".job-img__input");
    const outEl = card.querySelector(".job-img__output");
    const openBtn = card.querySelector(".btn-open");
    const dlBtn = card.querySelector(".btn-dl");

    try {
      const { url } = await JobClient.mySignedUrl(job.id, "input");
      inEl.src = url;
    } catch {}

    if (job.status === "completed" && job.output_path) {
      try {
        const { url } = await JobClient.mySignedUrl(job.id, "output");
        outEl.src = url;
        openBtn.href = url; openBtn.classList.remove("is-disabled");
        dlBtn.href = url; dlBtn.classList.remove("is-disabled"); dlBtn.removeAttribute("aria-disabled");
      } catch {}
    }
  }

  function updateJobCardState(card, ev) {
    // Update chip + show output on "completed"
    const chip = card.querySelector(".chip");
    const outEl = card.querySelector(".job-img__output");
    const openBtn = card.querySelector(".btn-open");
    const dlBtn = card.querySelector(".btn-dl");

    if (ev.type === "state") {
      chip.textContent = "PROCESSING"; chip.className = "chip chip--processing";
    } else if (ev.type === "error") {
      chip.textContent = "FAILED"; chip.className = "chip chip--failed";
    } else if (ev.type === "completed") {
      chip.textContent = "COMPLETED"; chip.className = "chip chip--completed";
      if (ev.output_url) {
        outEl.src = ev.output_url;
        openBtn.href = ev.output_url; openBtn.classList.remove("is-disabled");
        dlBtn.href = ev.output_url; dlBtn.classList.remove("is-disabled"); dlBtn.removeAttribute("aria-disabled");
      }
    }
  }

  async function tryResumeLast(beforeImg, afterImg, download) {
    try {
      const { items } = await JobClient.myJobs({ limit: 3 });
      if (!items || !items.length) return;

      const latest = items[0];
      // Preload input
      try {
        const { url } = await JobClient.mySignedUrl(latest.id, "input");
        beforeImg.src = url;
      } catch {}

      if (latest.status === "completed" && latest.output_path) {
        const { url } = await JobClient.mySignedUrl(latest.id, "output");
        afterImg.src = url;
        download.href = url;
        download.classList.remove("is-disabled");
        download.removeAttribute("aria-disabled");
        download.download = "kirkified.jpg";
        return;
      }

      if (latest.status === "processing" || latest.status === "queued") {
        HUD.show();
        HUD.mark("contact", "done");
        HUD.mark("start", "active");
        JobClient.on(latest.id, (ev) => {
          const type = ev.type; const msg = ev.message; const extra = ev.data || {};
          const queuePos = extra.queue_position ?? extra.position;
          if (typeof queuePos === "number") HUD.setQueue(queuePos);
          if (type === "state") {
            const m = String(msg || "").toLowerCase();
            if (m.includes("processing")) {
              HUD.mark("start", "done");
              HUD.mark("tunnel", "done");
              HUD.mark("models", "done");
              HUD.mark("process", "active");
            }
          } else if (type === "error") {
            alert(ev.message || "Processing failed");
            HUD.hide();
          } else if (type === "completed") {
            HUD.mark("process", "done");
            if (ev.output_url) {
              afterImg.src = ev.output_url;
              download.href = ev.output_url;
            }
            download.download = "kirkified.jpg";
            download.classList.remove("is-disabled");
            download.removeAttribute("aria-disabled");
            setTimeout(() => HUD.hide(), 400);
          }
        });
        JobClient.openEvents(latest.id);
        // Also fetch an ETA
        try {
          const wt = await waitTime();
          if (wt?.estimated_sec) HUD.setEta(wt.estimated_sec);
        } catch {}
      }
    } catch (e) {
      // silent resume failure is OK
    }
  }

  // ====== Uploader / UI glue ==================================================================
  function initUploader() {
    const drop = KirkApp.$("#drop");
    const uploadBtn = KirkApp.$("#upload-btn");
    const beforeImg = KirkApp.$("#before-img");
    const afterImg = KirkApp.$("#after-img");
    const download = KirkApp.$("#download-link");
    const refreshBtn = KirkApp.$("#refresh-jobs");

    if (!drop || !uploadBtn || !beforeImg || !afterImg || !download) {
      KirkApp.log("Uploader: missing node(s) – disabled");
      return;
    }

    let inflight = false;
    let currentJob = null;
    let currentES = null;

    function resetOutput() {
      afterImg.removeAttribute("src");
      download.href = "#";
      download.classList.add("is-disabled");
      download.setAttribute("aria-disabled", "true");
    }

    function safeDone() {
      inflight = false;
      HUD.hide();
      currentJob = null;
      HUD.setQueue(null);
      if (currentES) { currentES.close(); currentES = null; }
      // refresh "My Jobs" so it shows the new job immediately
      renderJobs();
    }

    async function handleFile(file) {
      if (!file || inflight) return;
      inflight = true;
      resetOutput();

      // show input immediately to BA slider
      beforeImg.src = URL.createObjectURL(file);

      // reset HUD
      KirkApp.$$(".hud__steps li").forEach((li) => li.classList.remove("is-active", "is-done"));
      HUD.show();
      HUD.mark("contact", "active");
      HUD.setQueue(null);
      HUD.setEta(null);

      try {
        const { job_id } = await JobClient.createJob(file);
        currentJob = job_id;
        HUD.mark("contact", "done");
        HUD.mark("start", "active");

        // show “come back later” + ETA
        try {
          const wt = await waitTime();
          if (wt?.estimated_sec) HUD.setEta(wt.estimated_sec);
        } catch {}

        // Listen to job events
        JobClient.on(job_id, (ev) => {
          const type = ev.type; const msg = ev.message; const extra = ev.data || {};
          const queuePos = extra.queue_position ?? extra.position;
          if (typeof queuePos === "number") {
            HUD.setQueue(queuePos);
            // refine ETA: estimate by position if we have it
            const cap = Math.max(1, Number(extra.capacity || 1)); // optional if we ever include it
            const ahead = Math.max(0, Number(queuePos) - 1);
            const est = Math.ceil((ahead / cap) * (KirkApp.AVG_JOB_SEC_FALLBACK));
            if (est > 0) HUD.setEta(est);
          }
          if (type === "state") {
            const m = String(msg || "").toLowerCase();
            if (m.includes("processing")) {
              HUD.mark("start", "done");
              HUD.mark("tunnel", "done");
              HUD.mark("models", "done");
              HUD.mark("process", "active");
            }
          } else if (type === "error") {
            alert(ev.message || "Processing failed");
            safeDone();
          } else if (type === "completed") {
            HUD.mark("process", "done");
            if (ev.output_url) {
              afterImg.src = ev.output_url;
              download.href = ev.output_url;
            } else if (ev.inline_base64) {
              afterImg.src = `data:image/jpeg;base64,${ev.inline_base64}`;
              download.href = afterImg.src;
            }
            download.download = "kirkified.jpg";
            download.classList.remove("is-disabled");
            download.removeAttribute("aria-disabled");
            setTimeout(() => safeDone(), 400);
          }
        });

        currentES = JobClient.openEvents(job_id);
      } catch (e) {
        KirkApp.log(e);
        alert("Upload failed: " + (e?.message || e || "Unknown error"));
        safeDone();
      }
    }

    // UI bindings
    KirkApp.on(uploadBtn, "click", () => {
      const input = document.createElement("input");
      input.type = "file"; input.accept = "image/*";
      input.onchange = () => handleFile(input.files?.[0]);
      input.click();
    });
    KirkApp.on(drop, "dragover", (e) => { e.preventDefault(); drop.classList.add("is-hover"); });
    KirkApp.on(drop, "dragleave", () => drop.classList.remove("is-hover"));
    KirkApp.on(drop, "drop", (e) => { e.preventDefault(); drop.classList.remove("is-hover"); handleFile(e.dataTransfer?.files?.[0]); });
    KirkApp.on(window, "paste", (e) => { const f = [...(e.clipboardData?.files || [])][0]; if (f) handleFile(f); });

    // "My Jobs" refresh button
    if (refreshBtn) KirkApp.on(refreshBtn, "click", renderJobs);

    // Try to resume previous job/result on load
    tryResumeLast(beforeImg, afterImg, download);
  }

  // ====== Boot ================================================================================
  document.addEventListener("DOMContentLoaded", () => {
    ensureClientId();
    initAllBeforeAfter();
    initUploader();
    loopGpuChip();
    renderJobs();
  });
  setInterval(loopGpuChip, 2000);
})();
