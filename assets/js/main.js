/* assets/js/main.js */
(() => {
  "use strict";

  // ====== KirkApp small utilities ============================================================
  const KirkApp = {
    CONTROLLER_URL: "https://api.keyauth.eu",
    AVG_JOB_SEC_FALLBACK: 75, // used if controller cannot compute a recent average
    MAX_JOBS_IN_UI: 12,
    log(...args) {
      const ts = new Date().toISOString();
      console.log(`[KirkApp ${ts}]`, ...args);
    },
    $: (sel, root = document) => root.querySelector(sel),
    $$: (sel, root = document) => Array.from(root.querySelectorAll(sel)),
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    on(el, ev, fn, opts) {
      if (el) el.addEventListener(ev, fn, opts);
    },
    fmtTime(tsIso) {
      try {
        const d = new Date(tsIso);
        return d.toLocaleString();
      } catch {
        return "";
      }
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
  const SHARE_COOKIE_KEY = "kirk_shared";

  function setCookie(name, value, maxAgeDays = 365) {
    const maxAge = maxAgeDays * 24 * 60 * 60;
    const secure = location.protocol === "https:" ? "; Secure" : "";
    document.cookie = `${encodeURIComponent(name)}=${encodeURIComponent(
      value
    )}; Path=/; Max-Age=${maxAge}; SameSite=Lax${secure}`;
  }

  function getCookie(name) {
    const safeName = name.replace(/[-[\]/{}()*+?.\\^$|]/g, "\\$&");
    const m = document.cookie.match(new RegExp(`(?:^|; )${safeName}=([^;]*)`));
    return m ? decodeURIComponent(m[1]) : "";
  }

  function ensureClientId() {
    let cid = localStorage.getItem(CLIENT_ID_KEY) || getCookie(CLIENT_ID_KEY);
    if (!cid) {
      cid =
        (window.crypto && crypto.randomUUID)
          ? crypto.randomUUID()
          : Math.random().toString(16).slice(2) + Date.now().toString(16);
      localStorage.setItem(CLIENT_ID_KEY, cid);
      setCookie(CLIENT_ID_KEY, cid, 365);
    } else {
      // keep cookie fresh even if only LS had it
      setCookie(CLIENT_ID_KEY, cid, 365);
      localStorage.setItem(CLIENT_ID_KEY, cid);
    }
    return cid;
  }

  // ====== Share gate (Web Share API) =========================================================
  function hasShared() {
    return getCookie(SHARE_COOKIE_KEY) === "1";
  }

  function markShared() {
    setCookie(SHARE_COOKIE_KEY, "1", 365);
  }

  function canUseWebShare() {
    return typeof navigator !== "undefined" && !!navigator.share;
  }

  /**
   * Ensures the user has "shared" before uploads are allowed.
   * - Shows a modal asking the user to share with 5 friends.
   * - When the Web Share API resolves successfully, we treat it as shared
   *   (we do NOT validate recipients) and unlock uploads permanently on this device.
   */
  async function ensureShareGate() {
    // Already unlocked on this device
    if (hasShared()) return true;

    // If Web Share API isn't available, fall back to a simple "copy link" style gate
    if (!canUseWebShare()) {
      const ok = window.confirm(
        "To keep Kirkify 100% free, please share this site with 5 friends. " +
          "Tap OK to copy the link, then share it and come back."
      );
      if (!ok) return false;

      try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          await navigator.clipboard.writeText(window.location.href);
        }
      } catch {
        // Ignore copy failures – we still let them through after confirming
      }

      markShared();
      return true;
    }

    const gate = KirkApp.$("#share-gate");
    const shareBtn = KirkApp.$("#share-gate-btn");
    const cancelBtn = KirkApp.$("#share-gate-cancel");

    // If gate UI is missing, just call navigator.share directly
    if (!gate || !shareBtn) {
      try {
        await navigator.share({
          title: "Kirkify – Charlie Kirk Face Swap",
          text: "Free Charlie Kirk face swap meme generator. Try it in your browser:",
          url: window.location.href,
        });
        // We don't validate recipients – any successful share counts as "5 people"
        markShared();
        return true;
      } catch (err) {
        console.error("Share canceled or failed", err);
        return false;
      }
    }

    gate.hidden = false;
    gate.classList.add("is-open");

    return new Promise((resolve) => {
      const cleanup = () => {
        gate.classList.remove("is-open");
        gate.hidden = true;
        shareBtn.removeEventListener("click", onShare);
        if (cancelBtn) cancelBtn.removeEventListener("click", onCancel);
      };

      const onShare = () => {
        navigator
          .share({
            title: "Kirkify – Charlie Kirk Face Swap",
            text: "Free Charlie Kirk face swap meme generator. Try it in your browser:",
            url: window.location.href,
          })
          .then(() => {
            // As soon as the share sheet completes successfully, we treat it
            // as if they shared with 5 people and unlock uploads permanently
            markShared();
            cleanup();
            resolve(true);
          })
          .catch((err) => {
            console.error("Share canceled or failed", err);
            // Keep the gate open; user can try again or cancel
          });
      };

      const onCancel = () => {
        cleanup();
        resolve(false);
      };

      shareBtn.addEventListener("click", onShare);
      if (cancelBtn) cancelBtn.addEventListener("click", onCancel);
    });
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

    const move = (e) => {
      const t = e.touches?.[0] || e;
      setPos(pctFromClientX(t.clientX));
      e.preventDefault?.();
    };

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

    if (range) {
      KirkApp.on(range, "input", (e) =>
        setPos(parseFloat(e.target.value || "50") || 50)
      );
    }

    setPos(50);
  }

  function initAllBeforeAfter() {
    KirkApp.$$(".ba").forEach(initBeforeAfter);
  }

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
    const liFor = (step) =>
      document.querySelector(`.hud__steps [data-step="${step}"]`);

    let active = false;
    let floating = false;
    let completed = false;

    function show() {
      if (!hud) return;
      active = true;
      floating = false;
      completed = false;
      hud.hidden = false;
      hud.classList.add("is-open");
      hud.classList.remove("hud--floating", "hud--completed");
      if (pb) pb.setAttribute("aria-valuenow", "0");
      if (comeBackEl) comeBackEl.hidden = false;
    }

    function hide() {
      if (!hud) return;
      active = false;
      floating = false;
      completed = false;
      hud.classList.remove("is-open", "hud--floating", "hud--completed");
      hud.classList.add("is-closing");
      setTimeout(() => {
        hud.hidden = true;
        hud.classList.remove("is-closing");
      }, 220);
      setQueue(null);
      setEta(null);
    }

    function float() {
      if (!hud || !active) return;
      floating = true;
      hud.classList.add("hud--floating");
    }

    function setCompleted() {
      if (!hud) return;
      completed = true;
      floating = true;
      hud.classList.add("hud--floating", "hud--completed");
    }

    function isActive() {
      return active;
    }

    function updateHudProgress() {
      if (!steps.length) return;
      const done = steps.filter((li) => li.classList.contains("is-done"))
        .length;
      const total = steps.length;
      const pct = Math.max(2, (done / total) * 100);
      if (bar) bar.style.width = pct + "%";
      if (pb) pb.setAttribute("aria-valuenow", String(done));
      if (live) {
        live.textContent = `Step ${Math.min(done + 1, total)} of ${total}`;
      }
    }

    function mark(step, state) {
      const li = liFor(step);
      if (!li) return;
      li.classList.remove("is-done", "is-active");
      if (state === "active") li.classList.add("is-active");
      if (state === "done") li.classList.add("is-done");
      updateHudProgress();
    }

    function setQueue(position) {
      if (!queueEl) return;
      if (!position || position <= 1) {
        queueEl.textContent = "";
        queueEl.hidden = true;
        return;
      }
      const ahead = position - 1;
      queueEl.hidden = false;
      queueEl.textContent = `Queue: you are #${position} (${ahead} ahead of you)…`;
    }

    function setEta(sec, text) {
      if (!etaEl) return;
      if ((sec == null || sec <= 0) && !text) {
        etaEl.textContent = "";
        etaEl.hidden = true;
        return;
      }
      etaEl.hidden = false;
      if (text) {
        etaEl.textContent = text;
      } else {
        etaEl.textContent = `Estimated wait: ~${KirkApp.fmtDuration(sec)}.`;
      }
    }

    function setJob(id) {
      const el = document.getElementById("hud-job");
      if (!el) return;
      el.hidden = false;
      el.textContent = `Job #${String(id).slice(
        0,
        8
      )} created. You can always find it later under “My Jobs”.`;
    }

    return {
      show,
      hide,
      float,
      setCompleted,
      isActive,
      mark,
      setQueue,
      setEta,
      setJob,
    };
  })();

  // ====== GPU chip loop + ETA source ==========================================================
  async function gpuStatus() {
    try {
      const res = await fetch(`${KirkApp.CONTROLLER_URL}/api/gpu_status`, {
        mode: "cors",
      });
      if (!res.ok) throw new Error("gpu_status failed");
      return await res.json();
    } catch (e) {
      return { status: "off" };
    }
  }

  async function waitTime() {
    try {
      const res = await fetch(`${KirkApp.CONTROLLER_URL}/api/wait_time`, {
        mode: "cors",
      });
      if (res.ok) return await res.json();
    } catch {
      // ignore
    }
    // fallback to chip math
    const s = await gpuStatus();
    const cap = Math.max(1, Number(s.total_capacity || 1));
    const ahead = Number(s.queued_jobs || 0) + Number(s.active_jobs || 0);
    const est = Math.ceil((ahead / cap) * KirkApp.AVG_JOB_SEC_FALLBACK);
    return {
      ok: true,
      avg_job_sec: KirkApp.AVG_JOB_SEC_FALLBACK,
      estimated_sec: est,
      source: "fallback",
    };
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
    // Map: jobId -> Set of handlers
    const events = {};

    function on(jobId, handler) {
      if (!events[jobId]) events[jobId] = new Set();
      events[jobId].add(handler);

      // Optional: return unsubscribe function
      return () => {
        events[jobId].delete(handler);
        if (events[jobId].size === 0) delete events[jobId];
      };
    }

    function openEvents(jobId) {
      const url = `${KirkApp.CONTROLLER_URL}/api/jobs/${jobId}/events`;
      const es = new EventSource(url, { withCredentials: false });

      es.onmessage = (ev) => {
        try {
          const payload = JSON.parse(ev.data || "{}");
          const handlers = events[jobId];
          if (handlers) {
            for (const fn of handlers) {
              try {
                fn(payload);
              } catch (e) {
                KirkApp.log("SSE handler error", e);
              }
            }
          }
        } catch (e) {
          KirkApp.log("SSE parse error", e);
        }
      };
      es.onerror = () => {
        // browser will retry
      };
      return es;
    }

    async function createJob(file) {
      const cid = ensureClientId();
      return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open("POST", `${KirkApp.CONTROLLER_URL}/api/jobs`, true);
        xhr.responseType = "json";
        xhr.setRequestHeader("X-Client-Id", cid);

        xhr.upload.onprogress = (e) => {
          if (e.lengthComputable) {
            const pct = Math.round((e.loaded / e.total) * 100);
            HUD.setEta(0, `Uploading image… ${pct}%`);
          }
        };
        xhr.onerror = () => reject(new Error("Network error uploading image."));
        xhr.onload = () => {
          const resp = xhr.response;
          if (xhr.status >= 200 && xhr.status < 300 && resp) {
            resolve({ job_id: resp.id, status: resp.status });
          } else {
            reject(
              new Error(
                `Upload failed: ${xhr.status} ${(resp && resp.detail) || ""}`
              )
            );
          }
        };

        const fd = new FormData();
        fd.append("file", file);
        xhr.send(fd);
      });
    }

    async function myJobs(params = {}) {
      const cid = ensureClientId();
      const qs = new URLSearchParams({
        ...params,
        limit: String(params.limit || KirkApp.MAX_JOBS_IN_UI),
      });
      const res = await fetch(
        `${KirkApp.CONTROLLER_URL}/api/my/jobs?${qs.toString()}`,
        {
          headers: { "X-Client-Id": cid },
          mode: "cors",
        }
      );
      if (!res.ok) return { items: [] };
      return res.json();
    }

    async function mySignedUrl(jobId, kind = "output") {
      const cid = ensureClientId();
      const qs = new URLSearchParams({
        job_id: jobId,
        kind,
        client_id: cid,
      });
      const res = await fetch(
        `${KirkApp.CONTROLLER_URL}/api/my/signed_url?${qs.toString()}`,
        { mode: "cors" }
      );
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
    wrap.innerHTML =
      '<div class="jobs-empty">Loading your recent jobs…</div>';
    try {
      const { items } = await JobClient.myJobs();
      if (!items || !items.length) {
        wrap.innerHTML =
          '<div class="jobs-empty">No recent jobs yet. Upload an image to get started.</div>';
        if (hint) hint.textContent = "";
        return;
      }
      wrap.innerHTML = "";
      if (hint) {
        hint.textContent =
          "Jobs are saved to this browser. You can close the tab and come back later.";
      }
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
      wrap.innerHTML =
        '<div class="jobs-empty">Failed to load jobs. Please try again.</div>';
    }
  }

  function jobCard(job) {
    const el = document.createElement("article");
    el.className = "job-card";
    el.dataset.job = job.id;

    let createdIso = "";
    if (job.created_at) {
      createdIso = job.created_at;
    } else if (job.created_at_ms) {
      try {
        createdIso = new Date(Number(job.created_at_ms)).toISOString();
      } catch {
        // ignore
      }
    }

    el.innerHTML = `
      <div class="job-head">
        <div class="job-title">
          <span class="chip chip--${job.status || "queued"}">
            ${(job.status || "queued").toUpperCase()}
          </span>
          <span class="job-id">#${job.id.slice(0, 8)}</span>
        </div>
        <div class="job-meta">
          ${createdIso ? KirkApp.fmtTime(createdIso) : ""}
        </div>
      </div>

      <div class="job-grid">
        <div class="job-img">
          <div class="job-img__label">Original</div>
          <img
            class="job-img__el job-img__input"
            alt="Original image for job ${job.id}"
          />
        </div>

        <div class="job-img">
          <div class="job-img__label">Kirkified</div>
          <img
            class="job-img__el job-img__output"
            alt="Kirkified image for job ${job.id}"
          />
        </div>
      </div>

      <div class="job-actions">
        <a class="btn-link btn-open is-disabled"
           target="_blank"
           aria-disabled="true">
          Open full image
        </a>
        <a class="btn-link btn-dl is-disabled"
           download="kirkified.jpg"
           aria-disabled="true">
          Download
        </a>
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
      if (inEl) inEl.src = url;
    } catch {
      // ignore
    }

    if (job.status === "completed" && job.output_path) {
      try {
        const { url } = await JobClient.mySignedUrl(job.id, "output");
        if (outEl) outEl.src = url;
        if (openBtn) {
          openBtn.href = url;
          openBtn.classList.remove("is-disabled");
        }
        if (dlBtn) {
          dlBtn.href = url;
          dlBtn.classList.remove("is-disabled");
          dlBtn.removeAttribute("aria-disabled");
        }
      } catch {
        // ignore
      }
    }
  }

  function updateJobCardState(card, ev) {
    // Update chip + show output on "completed"
    const chip = card.querySelector(".chip");
    const outEl = card.querySelector(".job-img__output");
    const openBtn = card.querySelector(".btn-open");
    const dlBtn = card.querySelector(".btn-dl");

    if (!chip) return;

    if (ev.type === "state") {
      chip.textContent = "PROCESSING";
      chip.className = "chip chip--processing";
    } else if (ev.type === "error") {
      chip.textContent = "FAILED";
      chip.className = "chip chip--failed";
    } else if (ev.type === "completed") {
      chip.textContent = "COMPLETED";
      chip.className = "chip chip--completed";
      if (ev.output_url && outEl) {
        outEl.src = ev.output_url;
        if (openBtn) {
          openBtn.href = ev.output_url;
          openBtn.classList.remove("is-disabled");
        }
        if (dlBtn) {
          dlBtn.href = ev.output_url;
          dlBtn.classList.remove("is-disabled");
          dlBtn.removeAttribute("aria-disabled");
        }
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
      } catch {
        // ignore
      }

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

        HUD.setJob(latest.id);
        EtaTicker.start();

        JobClient.on(latest.id, (ev) => {
          if (ev.type === "completed" || ev.type === "error") {
            EtaTicker.stop();
          }

          const type = ev.type;
          const msg = ev.message;
          const extra = ev.data || {};
          const queuePos =
            typeof extra.queue_position === "number"
              ? extra.queue_position
              : typeof extra.position === "number"
              ? extra.position
              : null;
          if (typeof queuePos === "number") {
            HUD.setQueue(queuePos);
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
          if (wt && wt.estimated_sec) {
            HUD.setEta(wt.estimated_sec);
          }
        } catch {
          // ignore
        }
      }
    } catch (e) {
      // silent resume failure is OK
    }
  }

  // ====== ETA Ticker (keeps ETA fresh while queued/processing) ================================
  const EtaTicker = (() => {
    let t = null;
    return {
      start() {
        if (t) clearInterval(t);
        t = setInterval(async () => {
          try {
            const wt = await waitTime();
            if (wt && wt.estimated_sec) {
              HUD.setEta(wt.estimated_sec);
            }
          } catch {
            // ignore
          }
        }, 15000);
      },
      stop() {
        if (t) {
          clearInterval(t);
          t = null;
        }
      },
    };
  })();

  // ====== Uploader / UI glue ==================================================================
  function initUploader() {
    const drop = KirkApp.$("#drop");
    const uploadBtn = KirkApp.$("#upload-btn");
    const beforeImg = KirkApp.$("#before-img");
    const afterImg = KirkApp.$("#after-img");
    const download = KirkApp.$("#download-link");
    const refreshBtn = KirkApp.$("#refresh-jobs");
    const scrollHint = KirkApp.$("#scroll-hint");


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
      EtaTicker.stop();
      inflight = false;
      HUD.hide();
      currentJob = null;
      HUD.setQueue(null);
      if (currentES) {
        currentES.close();
        currentES = null;
      }
      // refresh "My Jobs" so it shows the new job immediately
      renderJobs();
    }

    async function handleFile(file) {
      if (!file || inflight) return;
      if (scrollHint) scrollHint.hidden = true;

      inflight = true;
      resetOutput();

      // show input immediately to BA slider
      beforeImg.src = URL.createObjectURL(file);

      // reset HUD
      KirkApp.$$(".hud__steps li").forEach((li) => {
        li.classList.remove("is-active", "is-done");
      });
      HUD.show();
      HUD.mark("contact", "active");
      HUD.setQueue(null);
      HUD.setEta(null);

      try {
        const { job_id } = await JobClient.createJob(file);
        currentJob = job_id;
        HUD.setJob(job_id); // Show Job #
        renderJobs(); // Show the new job immediately in My Jobs
        EtaTicker.start(); // Keep ETA fresh while queued

        HUD.mark("contact", "done");
        HUD.mark("start", "active");

        // After a brief moment, shrink HUD into floating corner card
        setTimeout(() => {
          HUD.float();
        }, 450);

        // show “come back later” + initial ETA
        try {
          const wt = await waitTime();
          if (wt && wt.estimated_sec) HUD.setEta(wt.estimated_sec);
        } catch {
          // ignore
        }

        // Listen to job events
        JobClient.on(job_id, (ev) => {
          const type = ev.type;
          const msg = ev.message;
          const extra = ev.data || {};
          const queuePos =
            typeof extra.queue_position === "number"
              ? extra.queue_position
              : typeof extra.position === "number"
              ? extra.position
              : null;

          if (typeof queuePos === "number") {
            HUD.setQueue(queuePos);
            // refine ETA: estimate by position if we have it
            const cap = Math.max(
              1,
              Number(
                typeof extra.capacity === "number" ? extra.capacity : 1
              )
            ); // optional if we ever include it
            const ahead = Math.max(0, queuePos - 1);
            const est = Math.ceil(
              (ahead / cap) * KirkApp.AVG_JOB_SEC_FALLBACK
            );
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
            EtaTicker.stop();
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

            // Turn HUD into floating “done” chip with glow,
            // then quietly hide it after a few seconds.
            if (scrollHint) scrollHint.hidden = false;

            HUD.setCompleted();
            setTimeout(() => safeDone(), 6000);
          }
        });

        currentES = JobClient.openEvents(job_id);
      } catch (e) {
        KirkApp.log(e);
        alert(
          "Upload failed: " +
            ((e && e.message) || e || "Unknown error")
        );
        safeDone();
      }
    }

    // UI bindings
    KirkApp.on(uploadBtn, "click", async () => {
      // Gate uploads behind Web Share API "share with 5 friends"
      const ok = await ensureShareGate();
      if (!ok) {
        alert("You need to share Kirkify first before uploading an image.");
        return;
      }

      const input = document.createElement("input");
      input.type = "file";
      input.accept = "image/*";
      input.onchange = () => {
        const file =
          input.files && input.files.length ? input.files[0] : null;
        if (file) {
          handleFile(file);
        }
      };
      input.click();
    });

    KirkApp.on(drop, "dragover", (e) => {
      e.preventDefault();
      drop.classList.add("is-hover");
    });
    KirkApp.on(drop, "dragleave", () => drop.classList.remove("is-hover"));
    KirkApp.on(drop, "drop", async (e) => {
      e.preventDefault();
      drop.classList.remove("is-hover");
      const file =
        e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
      if (!file) return;

      const ok = await ensureShareGate();
      if (!ok) {
        alert("You need to share Kirkify first before uploading an image.");
        return;
      }

      handleFile(file);
    });

    KirkApp.on(window, "paste", async (e) => {
      const files =
        e.clipboardData && e.clipboardData.files
          ? Array.from(e.clipboardData.files)
          : [];
      const file = files[0];
      if (!file) return;

      const ok = await ensureShareGate();
      if (!ok) {
        alert("You need to share Kirkify first before uploading an image.");
        return;
      }

      handleFile(file);
    });

    // "My Jobs" refresh button
    if (refreshBtn) KirkApp.on(refreshBtn, "click", renderJobs);

    const AUTO_RESUME = false; // or read from env/config
    if (AUTO_RESUME) {
      tryResumeLast(beforeImg, afterImg, download);
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
  ensureClientId();
  initAllBeforeAfter();
  initUploader();
  loopGpuChip();
  renderJobs();

  const scrollHintEl = KirkApp.$("#scroll-hint");

  // If a job is in progress and user scrolls down, auto-float the HUD
  let floatedOnScroll = false;
  window.addEventListener(
    "scroll",
    () => {
      if (!floatedOnScroll && HUD.isActive() && window.scrollY > 120) {
        HUD.float();
        floatedOnScroll = true;
      }
      if (scrollHintEl && !scrollHintEl.hidden && window.scrollY > 40) {
        scrollHintEl.hidden = true;
      }
    },
    { passive: true }
  );
 });


  setInterval(loopGpuChip, 20000);
})();
