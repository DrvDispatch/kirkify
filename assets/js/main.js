/* assets/js/main.js */
(() => {
  "use strict";

  // ====== KirkApp small utilities ============================================================
  const KirkApp = {
    CONTROLLER_URL: "https://api.keyauth.eu",
    log(...args) { const ts = new Date().toISOString(); console.log(`[KirkApp ${ts}]`, ...args); },
    $: (sel, root = document) => root.querySelector(sel),
    $$: (sel, root = document) => Array.from(root.querySelectorAll(sel)),
    sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
    on(el, ev, fn, opts) { if (el) el.addEventListener(ev, fn, opts); },
  };

  // ====== persistent client id ===============================================================
  const CLIENT_ID_KEY = "kirk_cid";
  function ensureClientId() {
    let cid = localStorage.getItem(CLIENT_ID_KEY);
    if (!cid) {
      // Use crypto UUID when available
      cid = (window.crypto && crypto.randomUUID) ? crypto.randomUUID() :
            Math.random().toString(16).slice(2) + Date.now().toString(16);
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
    const liFor = (step) => document.querySelector(`.hud__steps [data-step="${step}"]`);

    function show() {
      if (!hud) return;
      hud.hidden = false; hud.classList.add("is-open");
      if (pb) pb.setAttribute("aria-valuenow", "0");
    }
    function hide() {
      if (!hud) return;
      hud.classList.remove("is-open");
      hud.classList.add("is-closing");
      setTimeout(() => { hud.hidden = true; hud.classList.remove("is-closing"); }, 220);
      setQueue(null);
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
    return { show, hide, mark, setQueue };
  })();

  // ====== GPU chip loop =======================================================================
  async function gpuStatus() {
    try {
      const res = await fetch(`${KirkApp.CONTROLLER_URL}/api/gpu_status`);
      if (!res.ok) throw new Error("gpu_status failed");
      return await res.json();
    } catch (e) {
      return { status: "off" };
    }
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
      });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`Job create failed: ${res.status} ${text}`);
      }
      const data = await res.json();
      return { job_id: data.id, status: data.status };
    }
    async function myJobs(params = {}) {
      const cid = ensureClientId();
      const qs = new URLSearchParams(params);
      const res = await fetch(`${KirkApp.CONTROLLER_URL}/api/my/jobs?` + qs.toString(), {
        headers: { "X-Client-Id": cid },
      });
      if (!res.ok) return { items: [] };
      return res.json();
    }
    async function mySignedUrl(jobId, kind = "output") {
      const cid = ensureClientId();
      const qs = new URLSearchParams({ job_id: jobId, kind, client_id: cid });
      const res = await fetch(`${KirkApp.CONTROLLER_URL}/api/my/signed_url?` + qs.toString());
      if (!res.ok) throw new Error("signed_url failed");
      return res.json();
    }
    return { createJob, openEvents, on, myJobs, mySignedUrl };
  })();

  async function tryResumeLast(beforeImg, afterImg, download) {
    try {
      const { items } = await JobClient.myJobs({ limit: 3 });
      if (!items || !items.length) return;

      // Pick the most recent job the user cares about
      const latest = items[0];
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
        // hook events
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
    }

    async function handleFile(file) {
      if (!file || inflight) return;
      inflight = true;
      resetOutput();
      beforeImg.src = URL.createObjectURL(file);

      KirkApp.$$(".hud__steps li").forEach((li) => li.classList.remove("is-active", "is-done"));

      HUD.show();
      HUD.mark("contact", "active");
      HUD.setQueue(null);

      try {
        const { job_id } = await JobClient.createJob(file);
        currentJob = job_id;
        HUD.mark("contact", "done");
        HUD.mark("start", "active");

        JobClient.on(job_id, (ev) => {
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
        alert("Upload failed: " + e.message);
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

    // Try to resume previous job/result on load
    tryResumeLast(beforeImg, afterImg, download);
  }

  // ====== Boot ============================================================================
  document.addEventListener("DOMContentLoaded", () => {
    initAllBeforeAfter();
    initUploader();
    loopGpuChip();
  });
  setInterval(loopGpuChip, 2000);
})();
