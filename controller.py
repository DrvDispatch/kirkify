import os, time, json, threading, subprocess, traceback
import requests
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import Response, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# -------------- CONFIG -----------------
load_dotenv()

VAST_ID = os.environ["VAST_INSTANCE_ID"]
GPU_WORKER_URL = os.environ["GPU_WORKER_URL"]
LOG_FILE = "controller.log"

gpu_state = {
    "status": "off",      # off | booting | ready
    "ttl": 0,
    "last_change": time.time()
}

TTL_INCREMENT = 60
TTL_MAX = 120
POLL_SEC = 10

# -------------- FASTAPI INIT -----------------
app = FastAPI()

# Enable CORS (optional: restrict allow_origins to your domain)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------- LOGGING UTILS -----------------
def log(msg: str):
    ts = time.strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{ts} {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# -------------- Vast CLI Wrappers -----------------
def vast(*args) -> subprocess.CompletedProcess:
    cmd = ["vastai", *args]
    log(f"[VAST] Running: {' '.join(cmd)}")
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if p.returncode != 0:
            log(f"[VAST] ERROR {p.returncode}: {p.stderr.strip()}")
        else:
            log(f"[VAST] OK: {p.stdout[:120].strip()}...")
        return p
    except Exception as e:
        log(f"[VAST] Exception: {e}")
        return subprocess.CompletedProcess(cmd, 1, "", str(e))

def vast_show_instance() -> dict:
    p = vast("show", "instance", VAST_ID, "--raw")

    raw = (p.stdout or "").strip()
    err = (p.stderr or "").strip()

    # ✅ If Vast CLI crashed but still printed JSON, trust stdout
    if raw.startswith("{") and ("Traceback" not in raw):
        try:
            info = json.loads(raw)
            log(f"[SYNC] Parsed Vast instance info OK (state={info.get('state') or info.get('status') or info.get('actual_status')})")
            return info
        except Exception as e:
            log(f"[SYNC] JSON parse failed ({e}). Raw head: {raw[:200]}")
            return {"state": "unknown"}

    # ❌ If stdout is empty or non-JSON, fallback to safe state
    if "Traceback" in raw or "Traceback" in err or not raw:
        log(f"[SYNC] Vast CLI traceback or invalid JSON. stderr len={len(err)}; stdout len={len(raw)}")
        return {"state": "running"}  # safe assumption to keep flow going

    try:
        info = json.loads(raw)
        return info
    except Exception as e:
        log(f"[SYNC] Unhandled parse error ({e}); returning running fallback.")
        return {"state": "running"}

def vast_start():
    log("[ACTION] Starting GPU instance via Vast.ai...")
    return vast("start", "instance", VAST_ID)

def vast_stop():
    log("[ACTION] Stopping GPU instance via Vast.ai...")
    return vast("stop", "instance", VAST_ID)

# -------------- GPU Lifecycle -----------------
def sync_gpu_state():
    """Sync local gpu_state with real Vast instance."""
    info = vast_show_instance()
    if not isinstance(info, dict):
        log("[SYNC] Invalid info structure from vast_show_instance()")
        return

    inst = None
    if "instances" in info and isinstance(info["instances"], list) and info["instances"]:
        inst = info["instances"][0]
    elif any(k in info for k in ("state", "status", "actual_status")):
        inst = info
    else:
        log("[SYNC] No valid instance data found (keys: " + ", ".join(info.keys()) + ")")
        return


    state = (inst.get("state") or inst.get("status") or inst.get("actual_status") or "").lower()
    log(f"[SYNC] Vast.ai reported raw state: {state}")

    if state in ("running", "active"):
        new_state = "ready"
    elif state in ("starting", "initializing", "creating"):
        new_state = "booting"
    else:
        new_state = "off"

    if gpu_state["status"] != new_state:
        log(f"[SYNC] Transition: {gpu_state['status']} → {new_state}")
        gpu_state["status"] = new_state
        gpu_state["last_change"] = time.time()
    else:
        log(f"[SYNC] State unchanged: {new_state}")

def ensure_gpu_running():
    """Ensure GPU is running, if not start it."""
    sync_gpu_state()
    if gpu_state["status"] == "off":
        log("[ENSURE] GPU is OFF → issuing Vast start()")
        gpu_state["status"] = "booting"
        gpu_state["last_change"] = time.time()
        vast_start()
    else:
        log(f"[ENSURE] GPU already {gpu_state['status']}, skipping start.")

def bump_ttl():
    old = gpu_state["ttl"]
    gpu_state["ttl"] = min(TTL_MAX, gpu_state["ttl"] + TTL_INCREMENT)
    log(f"[TTL] Bumped TTL from {old}s → {gpu_state['ttl']}s")

def ttl_tick():
    """Background thread to decrement TTL and stop GPU when expired."""
    while True:
        try:
            sync_gpu_state()

            if gpu_state["ttl"] > 0:
                gpu_state["ttl"] = max(0, gpu_state["ttl"] - POLL_SEC)
                log(f"[TTL] Decremented TTL → {gpu_state['ttl']}s remaining")

            if gpu_state["status"] == "ready" and gpu_state["ttl"] == 0 and gpu_state.get("last_ttl", 0) > 0:
                log("[TTL] TTL expired and GPU ready → stopping instance")
                vast_stop()
                gpu_state["status"] = "off"
                gpu_state["last_change"] = time.time()
                gpu_state["last_ttl"] = gpu_state["ttl"]

            elif gpu_state["status"] == "booting" and gpu_state["ttl"] == 0:
                log("[TTL] TTL expired but GPU still booting → will stop on next sync")

        except Exception:
            log("[ERROR] Exception in ttl_tick:\n" + traceback.format_exc())
        time.sleep(POLL_SEC)

threading.Thread(target=ttl_tick, daemon=True).start()

# -------------- API ROUTES -----------------
@app.post("/api/warm_gpu")
def warm_gpu():
    log("[API] /api/warm_gpu called")
    sync_gpu_state()
    bump_ttl()
    ensure_gpu_running()
    log(f"[API] Warm GPU response → status={gpu_state['status']} ttl={gpu_state['ttl']}")
    return {"ok": True, "status": gpu_state["status"], "ttl": gpu_state["ttl"]}
@app.get("/api/debug")
def debug():
    info = {
        "VAST_INSTANCE_ID": VAST_ID,
        "GPU_WORKER_URL": GPU_WORKER_URL,
        "gpu_state": gpu_state,
    }
    try:
        r = requests.get(f"{GPU_WORKER_URL}/health", timeout=2)
        info["worker_health"] = {"status_code": r.status_code, "ok": r.ok}
    except Exception as e:
        info["worker_health"] = {"error": str(e)}
    return info

@app.get("/api/gpu_status")
def gpu_status():
    log("[API] /api/gpu_status called")
    worker_ok = False
    try:
        r = requests.get(f"{GPU_WORKER_URL}/health", timeout=2)
        worker_ok = r.ok
        log(f"[HEALTH] GPU worker health={r.status_code}")
    except Exception as e:
        log(f"[HEALTH] GPU worker unreachable: {e}")
    sync_gpu_state()
    return {
        "status": gpu_state["status"],
        "ttl": gpu_state["ttl"],
        "worker_ok": worker_ok
    }

@app.post("/api/swap")
async def swap(file: UploadFile = File(...)):
    log("[API] /api/swap called")
    sync_gpu_state()
    if gpu_state["status"] != "ready":
        log(f"[SWAP] GPU not ready, current state={gpu_state['status']}")
        return JSONResponse({"waiting": True, "status": gpu_state["status"]}, status_code=425)

    bump_ttl()

    files = {"file": (file.filename, await file.read(), file.content_type or "application/octet-stream")}
    try:
        log(f"[SWAP] Forwarding image to GPU worker {GPU_WORKER_URL}")
        res = requests.post(f"{GPU_WORKER_URL}/api/swap", files=files, timeout=600)
        log(f"[SWAP] Worker response {res.status_code}")
        if not res.ok:
            log(f"[SWAP] Worker failed: {res.text[:200]}")
            return JSONResponse({"error": "worker_failed", "detail": res.text}, status_code=502)
        return Response(content=res.content, media_type=res.headers.get("content-type", "image/jpeg"))
    except requests.RequestException as e:
        log(f"[SWAP] Worker unreachable: {e}")
        return JSONResponse({"error": "worker_unreachable", "detail": str(e)}, status_code=502)

# -------------- MAIN -----------------
if __name__ == "__main__":
    import uvicorn
    log("Starting controller server...")
    uvicorn.run("controller:app", host="0.0.0.0", port=8000, reload=True)
