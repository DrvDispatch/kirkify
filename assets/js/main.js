(function () {
  function initBA(root) {
    const range = root.querySelector(".ba__range");

    const setPos = (pct) => {
      const clamped = Math.max(0, Math.min(100, pct));
      root.style.setProperty("--pos", clamped + "%");
      if (range) range.value = clamped;
    };

    const pctFromClientX = (clientX) => {
      const rect = root.getBoundingClientRect();
      return ((clientX - rect.left) / rect.width) * 100;
    };

    const move = (e) => {
      const clientX = e.touches ? e.touches[0].clientX : e.clientX;
      setPos(pctFromClientX(clientX));
      e.preventDefault?.();
    };

    const end = () => {
      window.removeEventListener("mousemove", move);
      window.removeEventListener("touchmove", move);
    };

    const start = (e) => {
      const clientX = e.touches ? e.touches[0].clientX : e.clientX;
      setPos(pctFromClientX(clientX));
      window.addEventListener("mousemove", move);
      window.addEventListener("touchmove", move);
      window.addEventListener("mouseup", end);
      window.addEventListener("touchend", end);
      e.preventDefault?.();
    };

    root.addEventListener("mousedown", start);
    root.addEventListener("touchstart", start, { passive: false });

    if (range) {
      range.addEventListener("input", (e) => setPos(parseFloat(e.target.value)));
    }

    setPos(50);
  }

  document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll(".ba").forEach((root) => initBA(root));
  });
})();

const CONTROLLER_URL = "https://api.keyauth.eu";

// tiny helpers
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const $  = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

function showHUD() {
  const hud = $("#hud");
  if (!hud) return;
  hud.hidden = false;
  hud.classList.add("is-open");
  const pb = $(".hud__progress");
  if (pb) pb.setAttribute("aria-valuenow", "0");
}
function hideHUD() {
  const hud = $("#hud");
  if (!hud) return;
  hud.classList.remove("is-open");
  hud.classList.add("is-closing");
  setTimeout(() => {
    hud.hidden = true;
    hud.classList.remove("is-closing");
  }, 220);
}
function updateProgress() {
  const steps = [...$$(".hud__steps li")];
  const done = steps.filter((li) => li.classList.contains("is-done")).length;
  const active = steps.findIndex((li) => li.classList.contains("is-active"));
  const total = steps.length;
  const pct = Math.max(2, (done / total) * 100);
  const bar = $(".hud__bar");
  if (bar) bar.style.width = pct + "%";
  const pb = $(".hud__progress");
  if (pb) pb.setAttribute("aria-valuenow", String(done));
  const live = $("#hud-live");
  if (live && active >= 0) {
    live.textContent = `Step ${Math.min(done + 1, total)} of ${total}`;
  }
}
function mark(step, state) {
  const li = document.querySelector(`.hud__steps [data-step="${step}"]`);
  if (!li) return;
  li.classList.remove("is-done", "is-active");
  if (state === "active") li.classList.add("is-active");
  if (state === "done")   li.classList.add("is-done");
  updateProgress();
}
async function fetchWithTimeout(url, opts = {}, ms = 15000) {
  const ctl = new AbortController();
  const id = setTimeout(() => ctl.abort(new Error("timeout")), ms);
  try { return await fetch(url, { ...opts, signal: ctl.signal }); }
  finally { clearTimeout(id); }
}
async function warmGPU() {
  try {
    const res = await fetchWithTimeout(`${CONTROLLER_URL}/api/warm_gpu`, { method: "POST" }, 15000);
    if (!res.ok) throw new Error("warm_gpu failed");
    return res.json();
  } catch (e) {
    try { await fetch(`${CONTROLLER_URL}/api/manual_start`, { method: "POST" }); } catch (_) {}
    throw e;
  }
}
async function gpuStatus() {
  const res = await fetchWithTimeout(`${CONTROLLER_URL}/api/gpu_status`, {}, 15000);
  if (!res.ok) throw new Error("gpu_status failed");
  return res.json();
}
async function uploadToSwap(file) {
  const form = new FormData();
  form.append("file", file);

  for (let attempt = 0; attempt < 60; attempt++) { // up to ~2 min retry window
    const res = await fetch(`${CONTROLLER_URL}/api/swap`, {
      method: "POST",
      body: form,
    });

    // Handle warming phase (HTTP 425)
    if (res.status === 425) {
      const j = await res.json().catch(() => ({}));
      console.log(`[warmup] GPU not ready yet (${j.status}/${j.phase})`);
      $("#gpu-status").textContent = j.status || "warming";
      mark("start", "done");
      mark("tunnel", "done");
      mark("models", "active");
      await sleep(2000);
      continue;
    }

    // Normal success
    if (res.ok) return await res.blob();

    // Other transient errors — wait and retry briefly
    if (res.status >= 500 && res.status < 600) {
      console.warn(`[retryable] ${res.status}`);
      await sleep(3000);
      continue;
    }

    // Hard error
    const t = await res.text().catch(() => "");
    throw new Error(`Upload failed (${res.status}) ${t}`);
  }

  throw new Error("GPU failed to warm up after 2 minutes. Please try again.");
}

async function loopGpuChip() {
  try {
    const s = await gpuStatus();
    const el = $("#gpu-status");
    el.textContent = s.status || "—";
    el.parentElement?.classList.toggle("is-offline", s.status !== "ready");
  } catch (_) {
    const el = $("#gpu-status");
    el.textContent = "offline";
    el.parentElement?.classList.add("is-offline");
  }
}
setInterval(loopGpuChip, 2000);
document.addEventListener("DOMContentLoaded", loopGpuChip);

document.addEventListener("DOMContentLoaded", () => {
  const drop = $("#drop");
  const uploadBtn = $("#upload-btn");
  const beforeImg = $("#before-img");
  const afterImg  = $("#after-img");
  const download  = $("#download-link");

  function handleFile(f) {
    if (!f) return;
    beforeImg.src = URL.createObjectURL(f);
    runPipeline(f).catch((e) => {
      console.error(e);
      hideHUD();
      alert("Something went wrong: " + e.message);
    });
  }
  uploadBtn.addEventListener("click", () => {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = "image/*";
    input.onchange = () => handleFile(input.files?.[0]);
    input.click();
  });
  drop.addEventListener("dragover", (e) => {
    e.preventDefault();
    drop.classList.add("is-hover");
  });
  drop.addEventListener("dragleave", () => drop.classList.remove("is-hover"));
  drop.addEventListener("drop", (e) => {
    e.preventDefault();
    drop.classList.remove("is-hover");
    handleFile(e.dataTransfer?.files?.[0]);
  });
  window.addEventListener("paste", (e) => {
    const f = [...(e.clipboardData?.files || [])][0];
    if (f) handleFile(f);
  });

  async function runPipeline(file) {
    $$(".hud__steps li").forEach((li) => (li.className = ""));
    showHUD();
    mark("contact", "active");

    const warm = await warmGPU();
    mark("contact", "done");

    if (warm.status === "ready") {
      mark("start",   "done");
      mark("tunnel",  "done");
      mark("models",  "done");
    } else {
      mark("start", "active");

      let ready = false;
      let tunnelShown = false, modelsShown = false;
      for (let i = 0; i < 120; i++) {
        const s = await gpuStatus().catch(() => ({ status: "off" }));
        $("#gpu-status").textContent = s.status || "—";

        if (s.status !== "off" && !tunnelShown) {
          mark("start", "done");
          mark("tunnel", "active");
          tunnelShown = true;
        }
        if (tunnelShown && !modelsShown && i > 5) {
          mark("tunnel", "done");
          mark("models", "active");
          modelsShown = true;
        }
        if (s.status === "ready") {
          mark("models", "done");
          ready = true;
          break;
        }
        await sleep(1000);
      }
      if (!ready) throw new Error("GPU is not ready yet");
    }

    mark("process", "active");
    const blob = await uploadToSwap(file);
    mark("process", "done");
    hideHUD();

    afterImg.src = URL.createObjectURL(blob);
    download.href = afterImg.src;
    download.download = "kirkified.jpg";
    download.classList.remove("is-disabled");
    download.removeAttribute("aria-disabled");
    download.classList.add("is-ready");
    setTimeout(() => download.classList.remove("is-ready"), 1800);
  }
});
document.addEventListener("DOMContentLoaded", () => {
  const indicator = document.querySelector(".scroll-indicator");
  const bar = document.querySelector(".scroll-progress-bar");
  const adSection = document.getElementById("ad-section");

  function updateProgress() {
    const rect = adSection.getBoundingClientRect();
    const totalHeight = adSection.offsetHeight;
    const scrolled = Math.min(Math.max(0, -rect.top), totalHeight);

    const pct = (scrolled / totalHeight) * 100;
    bar.style.width = pct + "%";

    // Optional auto-hide when done
    // if (pct >= 100) indicator.style.opacity = "0";
  }

  window.addEventListener("scroll", updateProgress);
  updateProgress();
});
