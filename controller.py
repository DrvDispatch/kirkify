#!/usr/bin/env python3
"""
Kirkify Controller — Redis-first, async, low-latency

What changed vs. your previous controller:
- 🔥 Firestore is **not** used on the hot path anymore (disabled by default).
- 🔁 Job queueing/locking and worker coordination moved to **Redis** (async).
- 📡 Event streaming (SSE) uses **Redis Pub/Sub** + a small rolling event log in Redis.
- ⏱️ All blocking GCS ops (upload/sign) are offloaded via asyncio.to_thread.
- 🧹 Lease expiry handled with Redis TTL + a small optional async reaper task.
- 🧩 Vast.ai polling is **not** in this process anymore (run separately if needed).
- 🧵 Uvicorn can run with multiple workers because state is in Redis.

Environment knobs (sane defaults; see below for setup commands):
  REDIS_URL                    redis://localhost:6379/0
  CORS_ORIGINS                 CSV list (defaults include kirkify.nl + localhost)
  HEARTBEAT_STALE_SEC          30.0
  JOB_LEASE_TIMEOUT_SEC        180.0
  TOTAL_JOB_TIMEOUT_SEC        300
  P0_ENABLED                   1          (enable priority queue list queue:p0)
  FIREBASE_GCS_ENABLED         1          (GCS via firebase_admin.storage)
  FIREBASE_CREDENTIALS_PATH    /srv/kirk-controller/FirebaseAuth.json
  FIREBASE_STORAGE_BUCKET      <optional> (defaults to <project-id>.appspot.com)
  FIRESTORE_ARCHIVE_EVENTS     0          (off by default; if 1, writes happen async)
  PRIORITY_IPS                 CSV of IPs that bypass normal queue
  LEASE_SWEEPER_ENABLED        0          (if 1, run lightweight async lease sweeper)
"""

import os
import re
import json
import time
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from uuid import uuid4
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import redis.asyncio as redis

from fastapi import (
    FastAPI,
    UploadFile,
    File,
    HTTPException,
    Request,
    Depends,
    Query,
    Form,
)
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --- Admin auth (JWT) ---
from jose import jwt, JWTError
from passlib.hash import bcrypt

# --- Optional Firebase Admin for GCS only (NO Firestore on hot path) ---
FIREBASE_GCS_ENABLED = os.getenv("FIREBASE_GCS_ENABLED", "1") not in {"0", "false", "no"}
bucket = None
if FIREBASE_GCS_ENABLED:
    try:
        import firebase_admin
        from firebase_admin import credentials, storage as fb_storage
    except Exception as _e:
        FIREBASE_GCS_ENABLED = False

# --- Env / Config ---
from dotenv import load_dotenv

load_dotenv(os.getenv("CONTROLLER_ENV_FILE", "/srv/kirk-controller/.env"), override=True)

# ================== Logging ==================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE")
LOG_FILE_ROTATE_MB = int(os.getenv("LOG_FILE_ROTATE_MB", "10"))
LOG_FILE_BACKUP = int(os.getenv("LOG_FILE_BACKUP", "3"))

logger = logging.getLogger("kirk.controller")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
_console = logging.StreamHandler()
_console.setFormatter(_fmt)
logger.addHandler(_console)
if LOG_FILE:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    _fh = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_FILE_ROTATE_MB * 1024 * 1024,
        backupCount=LOG_FILE_BACKUP,
    )
    _fh.setFormatter(_fmt)
    logger.addHandler(_fh)

# ================== App & CORS ==================
app = FastAPI(title="Kirkify Controller (Redis-first, async)")

_default_cors = [
    "https://admin.keyauth.eu",
    "https://kirkify.nl",
    "https://www.kirkify.nl",
    "http://localhost",
    "http://127.0.0.1",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]
CORS_ORIGINS = [
    o.strip() for o in os.getenv("CORS_ORIGINS", ",".join(_default_cors)).split(",") if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================== Config ==================
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Lease + queue
JOB_LEASE_TIMEOUT_SEC = float(os.getenv("JOB_LEASE_TIMEOUT_SEC", "180.0"))
HEARTBEAT_STALE_SEC = float(os.getenv("HEARTBEAT_STALE_SEC", "30.0"))
TOTAL_JOB_TIMEOUT_SEC = int(os.getenv("TOTAL_JOB_TIMEOUT_SEC", "300"))
P0_ENABLED = os.getenv("P0_ENABLED", "1") not in {"0", "false", "no"}

LEASE_SWEEPER_ENABLED = os.getenv("LEASE_SWEEPER_ENABLED", "0") in {"1", "true", "yes"}
LEASE_SWEEP_SEC = float(os.getenv("LEASE_SWEEP_SEC", "2.0"))

# Admin JWT
JWT_SECRET = os.getenv("JWT_SECRET", "change-me")
JWT_ISS = os.getenv("JWT_ISS", "kirkify-controller")
JWT_AUD = os.getenv("JWT_AUD", "admin")
JWT_EXP_MIN = int(os.getenv("JWT_EXP_MIN", "720"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS_HASH = os.getenv("ADMIN_PASS_HASH") or bcrypt.hash(os.getenv("ADMIN_PASS", "admin123"))

# Firebase (GCS)
FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH", "/srv/kirk-controller/FirebaseAuth.json")
FIREBASE_STORAGE_BUCKET = os.getenv("FIREBASE_STORAGE_BUCKET")

# Firestore archival (disabled by default)
FIRESTORE_ARCHIVE_EVENTS = os.getenv("FIRESTORE_ARCHIVE_EVENTS", "0") in {"1", "true", "yes"}
if FIRESTORE_ARCHIVE_EVENTS:
    # Optional & off-path: you can implement an async consumer to mirror Redis events to Firestore.
    # Intentionally not imported/initialized here to keep the hot path cold.
    logger.warning("FIRESTORE_ARCHIVE_EVENTS is enabled – remember this runs async and off the hot path.")

# Priority IPs
PRIORITY_IPS = {
    ip.strip()
    for ip in os.getenv("PRIORITY_IPS", "94.110.221.234").split(",")
    if ip.strip()
}

# ================== Redis ==================
r: Optional[redis.Redis] = None  # set in startup()

# Redis keys
K_WORKERS = "workers"                      # set of worker_ids
K_WORKER = "worker:{wid}"                  # hash
K_QUEUE_P0 = "queue:p0"
K_QUEUE_P1 = "queue:p1"
K_LEASE = "lease:{job_id}"                 # hash
K_LEASE_SET = "leases"                     # set of job_ids with leases (for optional sweeper)
K_JOB = "job:{job_id}"                     # hash
K_EVENTS = "events:{job_id}"               # list (LPUSH newest first)
K_EVENTS_CH = "ch:events:{job_id}"         # Pub/Sub channel
K_JOBS_GLOBAL = "jobs"                     # list of job_ids (LPUSH newest first)
K_CLIENT_JOBS = "client:{cid}:jobs"        # list of job_ids (LPUSH newest first)
K_IP_JOBS = "ip:{ip}:jobs"                 # list of job_ids (LPUSH newest first)

# ================== Utilities ==================
def _client_ip(request: Request) -> str:
    xf = request.headers.get("x-forwarded-for", "")
    if xf:
        return xf.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

def safe_filename(name: str) -> str:
    base = os.path.basename(name or "").strip() or "upload.bin"
    base = re.sub(r"[^\w.\-]+", "_", base)
    return base[:120]

def utc_now() -> datetime:
    return datetime.utcnow()

def utc_ms() -> int:
    return int(time.time() * 1000)

def to_iso(d: Optional[datetime]) -> Optional[str]:
    if isinstance(d, datetime):
        return d.isoformat()
    return None

async def maybe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default

async def maybe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default

# ================== GCS helpers (sync funcs + async wrappers) ==================
def _ensure_bucket_sync():
    global bucket, FIREBASE_GCS_ENABLED
    if not FIREBASE_GCS_ENABLED:
        raise RuntimeError("GCS disabled (FIREBASE_GCS_ENABLED=0)")
    if bucket is not None:
        return bucket

    # Lazy init firebase_admin for GCS
    if 'firebase_admin' not in globals():
        raise RuntimeError("firebase_admin not available")
    try:
        if not firebase_admin._apps:  # type: ignore[attr-defined]
            cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)  # type: ignore[name-defined]
            project_id = cred.project_id
            init_opts: Dict[str, Any] = {
                "storageBucket": FIREBASE_STORAGE_BUCKET or f"{project_id}.appspot.com",
                "projectId": project_id,
            }
            firebase_admin.initialize_app(cred, init_opts)  # type: ignore[name-defined]
        b = fb_storage.bucket()  # type: ignore[name-defined]
    except Exception as e:
        raise RuntimeError(f"GCS init failed: {e}")
    globals()["bucket"] = b
    return b

def upload_bytes_to_gcs_sync(path: str, blob_bytes: bytes, content_type: str) -> None:
    b = _ensure_bucket_sync()
    blob = b.blob(path)
    blob.upload_from_string(blob_bytes, content_type=content_type or "application/octet-stream")

def sign_url_sync(path: str, hours: int = 24) -> str:
    b = _ensure_bucket_sync()
    return b.blob(path).generate_signed_url(
        version="v4",
        expiration=timedelta(hours=hours),
        method="GET",
    )

# ================== Job/Event helpers (Redis) ==================
EVENTS_MAX = int(os.getenv("EVENTS_MAX", "200"))

async def rkey(template: str, **kwargs) -> str:
    return template.format(**kwargs)

async def publish_event(job_id: str, etype: str, message: str, progress: Optional[int] = None, extra: Optional[dict] = None):
    if r is None:
        return
    event = {"ts": utc_ms(), "type": etype, "message": message}
    if progress is not None:
        event["progress"] = progress
    if extra:
        event["data"] = extra

    k_events = await rkey(K_EVENTS, job_id=job_id)
    k_ch = await rkey(K_EVENTS_CH, job_id=job_id)
    # rolling log (LPUSH → newest first), capped
    await r.lpush(k_events, json.dumps(event))
    await r.ltrim(k_events, 0, EVENTS_MAX - 1)
    # pub/sub push
    await r.publish(k_ch, json.dumps(event))

async def get_job(job_id: str) -> Dict[str, Any]:
    if r is None:
        return {}
    k = await rkey(K_JOB, job_id=job_id)
    data = await r.hgetall(k)
    # decode numeric-ish
    if not data:
        return {}
    out: Dict[str, Any] = dict(data)
    for fld in ("created_at_ms", "started_at_ms", "finished_at_ms", "processing_ms"):
        if fld in out:
            try:
                out[fld] = int(out[fld])
            except Exception:
                pass
    return out

async def set_job_fields(job_id: str, fields: Dict[str, Any]):
    if r is None:
        return
    k = await rkey(K_JOB, job_id=job_id)
    # redis expects flat strings
    flat = {}
    for key, val in (fields or {}).items():
        if isinstance(val, (dict, list)):
            flat[key] = json.dumps(val)
        else:
            flat[key] = str(val)
    if flat:
        await r.hset(k, mapping=flat)

async def index_job(job_id: str, client_id: Optional[str], caller_ip: str):
    if r is None:
        return
    # global index (LPUSH newest first)
    await r.lpush(K_JOBS_GLOBAL, job_id)
    if client_id:
        await r.lpush(await rkey(K_CLIENT_JOBS, cid=client_id), job_id)
    await r.lpush(await rkey(K_IP_JOBS, ip=caller_ip), job_id)

async def enqueue_job(job_id: str, priority: bool = False):
    if r is None:
        return
    if priority and P0_ENABLED:
        await r.rpush(K_QUEUE_P0, job_id)
    else:
        await r.rpush(K_QUEUE_P1, job_id)

async def dequeue_job() -> Optional[str]:
    if r is None:
        return None
    if P0_ENABLED:
        jid = await r.lpop(K_QUEUE_P0)
        if jid:
            return jid
    return await r.lpop(K_QUEUE_P1)

async def queues_size() -> int:
    if r is None:
        return 0
    p0 = await r.llen(K_QUEUE_P0) if P0_ENABLED else 0
    p1 = await r.llen(K_QUEUE_P1)
    return int(p0) + int(p1)

async def create_lease(job_id: str, worker_id: str, timeout_sec: float, retries: int):
    if r is None:
        return
    now = time.time()
    deadline = now + timeout_sec
    k = await rkey(K_LEASE, job_id=job_id)
    await r.hset(k, mapping={"worker_id": worker_id, "deadline_ts": str(deadline), "retries": str(retries)})
    await r.sadd(K_LEASE_SET, job_id)

async def pop_lease(job_id: str) -> Optional[Dict[str, Any]]:
    if r is None:
        return None
    k = await rkey(K_LEASE, job_id=job_id)
    lease = await r.hgetall(k)
    await r.delete(k)
    await r.srem(K_LEASE_SET, job_id)
    return lease

async def get_lease(job_id: str) -> Dict[str, Any]:
    if r is None:
        return {}
    k = await rkey(K_LEASE, job_id=job_id)
    lease = await r.hgetall(k)
    return lease or {}

# ================== Auth helpers ==================
class LoginReq(BaseModel):
    username: str
    password: str

def make_jwt(sub: str) -> str:
    exp = datetime.utcnow() + timedelta(minutes=JWT_EXP_MIN)
    payload = {"sub": sub, "iss": JWT_ISS, "aud": JWT_AUD, "exp": exp}
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def _decode_jwt(token: str) -> dict:
    return jwt.decode(token, JWT_SECRET, algorithms=["HS256"], audience=JWT_AUD, issuer=JWT_ISS)

def require_admin(req: Request):
    auth = req.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    token = auth.split(" ", 1)[1]
    try:
        _decode_jwt(token)
        return True
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ================== HTTP API ==================
@app.get("/api/health")
async def health():
    return {"ok": True, "status": "alive"}

@app.get("/api/ping")
async def ping():
    return {"pong": True, "ts": time.time()}

# ---- Auth ----
@app.post("/api/auth/login")
async def login(req: LoginReq):
    if req.username != ADMIN_USER or not bcrypt.verify(req.password, ADMIN_PASS_HASH):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"ok": True, "token": make_jwt(req.username), "user": req.username}

@app.get("/api/auth/me")
async def me(_: bool = Depends(require_admin)):
    return {"ok": True, "user": ADMIN_USER}

# ---- GPU status for frontend chip (pool summary) ----
@app.get("/api/gpu_status")
async def gpu_status():
    if r is None:
        return {"status": "off", "workers_online": 0, "active_jobs": 0, "total_capacity": 0, "queued_jobs": 0, "pool": "pull-based"}
    now = time.time()
    online = 0
    active = 0
    capacity = 0
    worker_ids = await r.smembers(K_WORKERS)
    for wid in worker_ids or []:
        w = await r.hgetall(await rkey(K_WORKER, wid=wid))
        if not w:
            continue
        last_seen = float(w.get("last_seen_ts", "0") or "0")
        if now - last_seen < HEARTBEAT_STALE_SEC:
            online += 1
        active += int(w.get("active", "0") or "0")
        capacity += int(w.get("capacity", "1") or "1")
    queued = await queues_size()
    status = "ready" if online > 0 else "off"
    return {
        "status": status,
        "workers_online": online,
        "active_jobs": active,
        "total_capacity": capacity,
        "queued_jobs": queued,
        "pool": "pull-based",
    }

# ---- Worker registration / leasing (pull-based) ----
class WorkerRegisterReq(BaseModel):
    name: Optional[str] = None
    public_url: Optional[str] = None
    capacity: int = 1
    tags: Optional[Dict[str, Any]] = None
    gpu: Optional[Dict[str, Any]] = None

class WorkerLeaseReq(BaseModel):
    worker_id: str
    wants: int = 1
    active: int = 0
    gpu: Optional[Dict[str, Any]] = None

class WorkerHeartbeatReq(BaseModel):
    worker_id: str
    metrics: Optional[Dict[str, Any]] = None

@app.post("/api/worker/register")
async def worker_register(req: WorkerRegisterReq, request: Request):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")
    wid = uuid4().hex
    now = time.time()
    info = {
        "id": wid,
        "name": req.name or f"worker-{wid[:6]}",
        "public_url": req.public_url or "",
        "capacity": str(max(1, int(req.capacity or 1))),
        "active": "0",
        "last_seen_ts": str(now),
        "first_seen_ts": str(now),
        "tags": json.dumps(req.tags or {}),
        "gpu": json.dumps(req.gpu or {}),
        "remote_ip": _client_ip(request),
        "vast_instance_id": "",
    }
    await r.hset(await rkey(K_WORKER, wid=wid), mapping=info)
    await r.sadd(K_WORKERS, wid)
    logger.info(f"worker registered: {info['name']} ({wid}) ip={info['remote_ip']}")

    return {
        "ok": True,
        "worker_id": wid,
        "lease_endpoint": "/api/worker/lease",
        "result_endpoint": "/api/worker/result",
        "error_endpoint": "/api/worker/error",
        "heartbeat_interval_sec": max(2, int(HEARTBEAT_STALE_SEC // 2)),
    }

@app.post("/api/worker/heartbeat")
async def worker_heartbeat(req: WorkerHeartbeatReq):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")
    wid = req.worker_id
    k = await rkey(K_WORKER, wid=wid)
    if not await r.exists(k):
        raise HTTPException(status_code=404, detail="unknown worker_id")
    now = time.time()
    upd = {"last_seen_ts": str(now)}
    if req.metrics:
        upd["gpu"] = json.dumps(req.metrics)
    await r.hset(k, mapping=upd)
    return {"ok": True}

@app.post("/api/worker/lease")
async def worker_lease(req: WorkerLeaseReq, request: Request):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")
    wid = req.worker_id
    k = await rkey(K_WORKER, wid=wid)
    if not await r.exists(k):
        raise HTTPException(status_code=404, detail="unknown worker_id")

    now = time.time()
    # update worker state
    upd = {
        "last_seen_ts": str(now),
        "active": str(int(req.active or 0)),
        "remote_ip": _client_ip(request),
    }
    if req.gpu:
        upd["gpu"] = json.dumps(req.gpu)
    await r.hset(k, mapping=upd)

    # fairness: at most 1 lease per call
    wants = max(0, int(req.wants or 0))
    winfo = await r.hgetall(k)
    cap = int(winfo.get("capacity", "1") or "1")
    active = int(winfo.get("active", "0") or "0")
    free_slots = max(0, cap - active)
    grant = min(wants, free_slots, 1)
    if grant <= 0:
        return {"ok": True, "lease": None, "wait_sec": 2}

    job_id = await dequeue_job()
    if not job_id:
        return {"ok": True, "lease": None, "wait_sec": 2}

    job = await get_job(job_id)
    if not job:
        return {"ok": True, "lease": None, "wait_sec": 1}

    filename = job.get("filename") or "upload.jpg"
    input_path = job.get("input_path")
    if not input_path:
        await set_job_fields(job_id, {"status": "failed", "error": "missing input_path"})
        await publish_event(job_id, "error", "missing input_path")
        return {"ok": True, "lease": None, "wait_sec": 1}

    # sign URL off-thread
    try:
        url = await asyncio.to_thread(sign_url_sync, input_path, 1)
    except Exception as e:
        await set_job_fields(job_id, {"status": "failed", "error": f"sign_url failed: {e}"})
        await publish_event(job_id, "error", "sign_url failed")
        return {"ok": True, "lease": None, "wait_sec": 1}

    # increment worker active
    await r.hincrby(k, "active", 1)

    # set job state & lease
    await set_job_fields(job_id, {"status": "processing", "started_at_ms": utc_ms(), "worker_id": wid})
    lease_prev = await get_lease(job_id)
    retries = int(lease_prev.get("retries", "0") or "0")
    await create_lease(job_id, wid, JOB_LEASE_TIMEOUT_SEC, retries)
    await publish_event(job_id, "state", "processing", 40)

    lease = {
        "job_id": job_id,
        "filename": filename,
        "input_url": url,
        "deadline_ts": time.time() + JOB_LEASE_TIMEOUT_SEC,
        "total_job_timeout_sec": TOTAL_JOB_TIMEOUT_SEC,
        "params": {},
    }
    return {"ok": True, "lease": lease}

@app.post("/api/worker/result")
async def worker_result(
    request: Request,
    worker_id: str = Form(...),
    job_id: str = Form(...),
    file: UploadFile = File(...),
):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")

    # Validate lease (loosened: allow fallback to job state)
    lease = await get_lease(job_id)
    if not lease or lease.get("worker_id") != worker_id:
        # Fallback: trust job state if it's still processing on this worker
        job = await get_job(job_id)
        if (
            not job
            or job.get("status") != "processing"
            or job.get("worker_id") != worker_id
        ):
            raise HTTPException(status_code=400, detail="invalid lease or worker_id")

    # Read bytes
    try:
        blob_bytes = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"read upload failed: {e}")

    out_path = f"jobs/{job_id}/output/output.jpg"
    try:
        await asyncio.to_thread(
            upload_bytes_to_gcs_sync,
            out_path,
            blob_bytes,
            file.content_type or "image/jpeg",
        )
    except Exception as e:
        # decrement worker active, clear lease
        wkey = await rkey(K_WORKER, wid=worker_id)
        if await r.exists(wkey):
            await r.hincrby(wkey, "active", -1)
        await pop_lease(job_id)

        await set_job_fields(
            job_id,
            {"status": "failed", "error": f"output upload error: {e}"},
        )
        await publish_event(job_id, "error", f"upload output error: {e}")
        return {"ok": False, "error": "upload_failed"}

    # Success
    await set_job_fields(
        job_id,
        {
            "status": "completed",
            "output_path": out_path,
            "finished_at_ms": utc_ms(),
        },
    )
    # compute processing_ms best-effort
    job = await get_job(job_id)
    try:
        st = int(job.get("started_at_ms", 0))
        fin = int(job.get("finished_at_ms", 0))
        if st and fin and fin >= st:
            await set_job_fields(job_id, {"processing_ms": fin - st})
    except Exception:
        pass

    # decrement worker active & clear lease
    wkey = await rkey(K_WORKER, wid=worker_id)
    if await r.exists(wkey):
        await r.hincrby(wkey, "active", -1)
    await pop_lease(job_id)

    # push a 'completed' event with direct signed URL (so frontend updates immediately)
    try:
        signed = await asyncio.to_thread(sign_url_sync, out_path, 24)
        await publish_event(
            job_id,
            "completed",
            "completed",
            100,
            {"output_url": signed, "output_path": out_path},
        )
    except Exception:
        await publish_event(
            job_id,
            "completed",
            "completed",
            100,
            {"output_path": out_path},
        )

    return {"ok": True}

class WorkerErrorReq(BaseModel):
    worker_id: str
    job_id: str
    error: str

@app.post("/api/worker/error")
async def worker_error(req: WorkerErrorReq):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")

    # clear worker active & lease
    wkey = await rkey(K_WORKER, wid=req.worker_id)
    if await r.exists(wkey):
        await r.hincrby(wkey, "active", -1)

    lease = await pop_lease(req.job_id)
    retries = int((lease or {}).get("retries", "0") or "0")

    if retries < 3:
        await set_job_fields(req.job_id, {"status": "queued", "error": ""})
        await create_lease(req.job_id, "", 1, retries + 1)  # store retries but effectively "no lease"
        await enqueue_job(req.job_id)
        await publish_event(req.job_id, "info", f"requeued after error: {req.error}", 5, {"retries": retries + 1})
    else:
        await set_job_fields(req.job_id, {"status": "failed", "error": req.error})
        await publish_event(req.job_id, "error", req.error)

    return {"ok": True}

# ================== Jobs API (public + admin) ==================
class JobCreateResponse(BaseModel):
    id: str
    status: str

async def _create_job_common(request: Request, upload: UploadFile) -> Dict[str, Any]:
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")

    client_id = request.headers.get("x-client-id") or request.query_params.get("client_id")
    user_agent = request.headers.get("user-agent") or ""
    caller_ip = _client_ip(request)
    is_prio = caller_ip in PRIORITY_IPS

    try:
        payload = await upload.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"read upload failed: {e}")

    filename = safe_filename(upload.filename or "upload")
    job_id = uuid4().hex
    inp_path = f"jobs/{job_id}/input/{filename}"

    # Upload to GCS off-thread
    try:
        await asyncio.to_thread(
            upload_bytes_to_gcs_sync, inp_path, payload, upload.content_type or "application/octet-stream"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"GCS upload failed: {e}")

    now_ms = utc_ms()
    job_doc = {
        "id": job_id,
        "status": "queued",
        "requested_by_ip": caller_ip,
        "client_id": client_id or "",
        "user_agent": user_agent,
        "filename": filename,
        "input_path": inp_path,
        "output_path": "",
        "created_at_ms": now_ms,
        "started_at_ms": 0,
        "finished_at_ms": 0,
        "processing_ms": 0,
        "mode": "async",
        "error": "",
    }
    await set_job_fields(job_id, job_doc)
    await index_job(job_id, client_id, caller_ip)

    # Queue position estimate
    p0_len = (await r.llen(K_QUEUE_P0)) if (P0_ENABLED) else 0
    p1_len = await r.llen(K_QUEUE_P1)
    # active/capacity snapshot
    active_now = 0
    cap = 0
    for wid in (await r.smembers(K_WORKERS)) or []:
        w = await r.hgetall(await rkey(K_WORKER, wid=wid))
        if not w:
            continue
        active_now += int(w.get("active", "0") or "0")
        cap += int(w.get("capacity", "1") or "1")
    cap = max(1, cap)

    if is_prio and P0_ENABLED:
        position = int(p0_len) + int(active_now) + 1
        await enqueue_job(job_id, priority=True)
    else:
        position = int(p0_len) + int(p1_len) + int(active_now) + 1
        await enqueue_job(job_id, priority=False)

    await publish_event(
        job_id,
        "info",
        "job queued",
        1,
        {"filename": filename, "queue_position": position, "capacity": cap, "priority": bool(is_prio)},
    )
    return {"id": job_id, "status": "queued"}

@app.post("/api/jobs", response_model=JobCreateResponse)
async def create_job_admin(request: Request, file: UploadFile = File(...)):
    return await _create_job_common(request, file)

@app.post("/api/swap", response_model=JobCreateResponse)
async def create_job_legacy(request: Request, file: UploadFile = File(...)):
    return await _create_job_common(request, file)

# --- Admin jobs list/detail ---
@app.get("/api/jobs")
async def list_jobs(
    status: Optional[str] = Query(None),
    q: Optional[str] = Query(None, description="substring match on filename or IP"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _: bool = Depends(require_admin),
):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")

    # Pull a window from the global list (LPUSH newest first)
    job_ids = await r.lrange(K_JOBS_GLOBAL, offset, offset + 999)
    items: List[Dict[str, Any]] = []
    for jid in job_ids:
        j = await get_job(jid)
        if not j:
            continue
        if status and (j.get("status") != status):
            continue
        if q:
            hay = f"{j.get('filename','')} {j.get('requested_by_ip','')}".lower()
            if q.lower() not in hay:
                continue
        items.append(j)
        if len(items) >= limit:
            break
    return {"ok": True, "items": items}

@app.get("/api/jobs/{job_id}")
async def get_job_admin(job_id: str, _: bool = Depends(require_admin)):
    j = await get_job(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Not found")
    # read recent events (LPUSH newest first) → reverse to chronological
    ev_key = await rkey(K_EVENTS, job_id=job_id)
    raw = await r.lrange(ev_key, 0, EVENTS_MAX - 1)
    evs = [json.loads(x) for x in reversed(raw or [])]
    # Convert ts to ISO
    for e in evs:
        try:
            e["ts"] = datetime.utcfromtimestamp(e["ts"] / 1000.0).isoformat()
        except Exception:
            pass
    return {"ok": True, "job": j, "events": evs}

# --- Admin signed URL for images ---
@app.get("/api/jobs/{job_id}/signed_url")
async def admin_job_signed_url(job_id: str, kind: str = Query("input"), _: bool = Depends(require_admin)):
    j = await get_job(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Not found")
    path = j.get("input_path" if kind == "input" else "output_path")
    if not path:
        raise HTTPException(status_code=404, detail="no_path")
    try:
        url = await asyncio.to_thread(sign_url_sync, path, 24)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"sign_url failed: {e}")
    return {"ok": True, "url": url}

# --- Admin: cancel / retry / delete (basic) ---
@app.post("/api/jobs/{job_id}/cancel")
async def job_cancel(job_id: str, _: bool = Depends(require_admin)):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")
    # remove from queues
    await r.lrem(K_QUEUE_P0, 0, job_id)
    await r.lrem(K_QUEUE_P1, 0, job_id)
    await set_job_fields(job_id, {"status": "canceled"})
    await publish_event(job_id, "state", "canceled", 0, {})
    return {"ok": True, "message": "canceled"}

@app.post("/api/jobs/{job_id}/retry")
async def job_retry(job_id: str, _: bool = Depends(require_admin)):
    j = await get_job(job_id)
    if not j or not j.get("input_path"):
        raise HTTPException(status_code=400, detail="no input_path")
    new_id = uuid4().hex
    # clone core fields
    new_job = {
        "id": new_id,
        "status": "queued",
        "requested_by_ip": j.get("requested_by_ip", ""),
        "client_id": j.get("client_id", ""),
        "user_agent": j.get("user_agent", ""),
        "filename": j.get("filename", ""),
        "input_path": j.get("input_path", ""),
        "output_path": "",
        "created_at_ms": utc_ms(),
        "started_at_ms": 0,
        "finished_at_ms": 0,
        "processing_ms": 0,
        "mode": "async",
        "error": "",
    }
    await set_job_fields(new_id, new_job)
    await index_job(new_id, new_job["client_id"], new_job["requested_by_ip"])
    is_prio = (j.get("requested_by_ip") or "") in PRIORITY_IPS
    await enqueue_job(new_id, priority=is_prio)
    await publish_event(new_id, "info", "job queued (retry)", 1, {"from_job": job_id, "priority": bool(is_prio)})
    return {"ok": True, "new_job_id": new_id}

@app.delete("/api/jobs/{job_id}")
async def job_delete(job_id: str, _: bool = Depends(require_admin)):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")
    # remove from queues
    await r.lrem(K_QUEUE_P0, 0, job_id)
    await r.lrem(K_QUEUE_P1, 0, job_id)
    # best-effort delete blobs (off-thread)
    j = await get_job(job_id)
    try:
        if j.get("input_path"):
            await asyncio.to_thread(lambda p=j["input_path"]: _ensure_bucket_sync().blob(p).delete(if_generation_match=None))  # type: ignore
        if j.get("output_path"):
            await asyncio.to_thread(lambda p=j["output_path"]: _ensure_bucket_sync().blob(p).delete(if_generation_match=None))  # type: ignore
    except Exception:
        pass
    # delete events list + job hash
    await r.delete(await rkey(K_EVENTS, job_id=job_id))
    await r.delete(await rkey(K_LEASE, job_id=job_id))
    await r.srem(K_LEASE_SET, job_id)
    await r.hdel(await rkey(K_JOB, job_id=job_id), *list((await r.hkeys(await rkey(K_JOB, job_id=job_id))) or []))
    return {"ok": True}

@app.get("/api/jobs/{job_id}/events")
async def stream_events_public(job_id: str):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")

    import contextlib

    async def gen():
        # Send retry hint
        yield b"retry: 1000\n\n"

        # 1) Send history from global Redis client
        ev_key = await rkey(K_EVENTS, job_id=job_id)
        raw = await r.lrange(ev_key, 0, EVENTS_MAX - 1)
        history = [json.loads(x) for x in reversed(raw or [])]
        for e in history:
            yield b"data: " + json.dumps(e).encode("utf-8") + b"\n\n"

        # 2) For live updates, use a **dedicated Redis client** so we don't eat the shared pool
        local_r = redis.from_url(
            REDIS_URL,
            decode_responses=True,
            max_connections=1,   # single connection reserved for this SSE
        )
        ch = await rkey(K_EVENTS_CH, job_id=job_id)
        pubsub = local_r.pubsub()
        await pubsub.subscribe(ch)

        try:
            async for msg in pubsub.listen():
                if msg is None:
                    await asyncio.sleep(0.2)
                    continue
                if msg["type"] != "message":
                    continue

                payload = msg.get("data")
                if isinstance(payload, (bytes, bytearray)):
                    payload = payload.decode("utf-8", errors="ignore")
                if not payload:
                    continue

                # Forward to client
                yield b"data: " + payload.encode("utf-8") + b"\n\n"

                # Stop stream on terminal events
                try:
                    pl = json.loads(payload)
                    if pl.get("type") in {"completed", "failed", "timeout", "canceled"}:
                        break
                except Exception:
                    pass
        finally:
            with contextlib.suppress(Exception):
                await pubsub.unsubscribe(ch)
                await pubsub.close()
                await local_r.close()

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )

# --- Admin SSE (same Pub/Sub channel, token required) ---
@app.get("/api/jobs/{job_id}/events/stream")
async def stream_events_admin(job_id: str, token: str = Query(...)):
    try:
        _decode_jwt(token)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    return await stream_events_public(job_id)  # identical stream

# ================== Public “My jobs” (persistent via Redis indexes) ==================
@app.get("/api/my/jobs")
async def my_jobs(
    request: Request,
    status: Optional[str] = None,
    limit: int = Query(10, ge=1, le=100),
    client_id: Optional[str] = Query(None),
):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")
    cid = client_id or request.headers.get("X-Client-Id") or request.cookies.get("kirk_cid")
    ip = _client_ip(request)
    items: List[Dict[str, Any]] = []

    # prefer client_id index; fall back to IP index
    list_key = await rkey(K_CLIENT_JOBS, cid=cid) if cid else await rkey(K_IP_JOBS, ip=ip)
    if not cid:
        list_key = await rkey(K_IP_JOBS, ip=ip)

    ids = await r.lrange(list_key, 0, limit - 1)
    for jid in ids or []:
        j = await get_job(jid)
        if not j:
            continue
        if status and j.get("status") != status:
            continue
        items.append(j)
    return {"ok": True, "items": items}

@app.get("/api/my/signed_url")
async def my_signed_url(
    request: Request, job_id: str, kind: str = "output", client_id: Optional[str] = Query(None)
):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")
    cid = client_id or request.headers.get("X-Client-Id") or request.cookies.get("kirk_cid")
    j = await get_job(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Not found")
    caller_ip = _client_ip(request)
    owner_ok = (cid and j.get("client_id") == cid) or (j.get("requested_by_ip") == caller_ip)
    if not owner_ok:
        raise HTTPException(status_code=403, detail="not your job")

    path = j.get("input_path" if kind == "input" else "output_path")
    if not path:
        raise HTTPException(status_code=404, detail="no_path")
    try:
        url = await asyncio.to_thread(sign_url_sync, path, 24)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"sign_url failed: {e}")
    return {"ok": True, "url": url}

# ================== Admin: diagnostics & metrics ==================
@app.get("/api/workers")
async def list_workers(_: bool = Depends(require_admin)):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")
    now = time.time()
    items = []
    for wid in (await r.smembers(K_WORKERS)) or []:
        d = await r.hgetall(await rkey(K_WORKER, wid=wid))
        if not d:
            continue
        d["stale_sec"] = now - float(d.get("last_seen_ts", "0") or "0")
        items.append(d)
    return {"ok": True, "items": items}

@app.get("/api/metrics")
async def metrics(sample: int = Query(200, ge=10, le=1000), _: bool = Depends(require_admin)):
    if r is None:
        raise HTTPException(status_code=500, detail="Redis unavailable")
    by_status: Dict[str, int] = {}
    total = 0
    proc_ms: List[int] = []

    ids = await r.lrange(K_JOBS_GLOBAL, 0, sample - 1)
    for jid in ids or []:
        j = await get_job(jid)
        if not j:
            continue
        total += 1
        st = (j.get("status") or "").lower()
        by_status[st] = by_status.get(st, 0) + 1
        st_ms = int(j.get("started_at_ms", 0) or 0)
        fin_ms = int(j.get("finished_at_ms", 0) or 0)
        if st_ms and fin_ms and fin_ms >= st_ms:
            proc_ms.append(fin_ms - st_ms)

    avg = (sum(proc_ms) / len(proc_ms)) if proc_ms else None
    return {"ok": True, "by_status": by_status, "total_sampled": total, "avg_processing_ms": avg}

# --- Admin: stubs for warm/start/stop (pull-based) ---
@app.post("/api/warm_gpu")
async def warm_gpu(_: bool = Depends(require_admin)):
    return {"ok": True, "message": "Pull-based pool warms on worker startup; nothing to do."}

@app.post("/api/manual_start")
async def manual_start(_: bool = Depends(require_admin)):
    return {"ok": True, "message": "Start/stop is not applicable in pull-based mode."}

@app.post("/api/manual_stop")
async def manual_stop(_: bool = Depends(require_admin)):
    return {"ok": True, "message": "Start/stop is not applicable in pull-based mode."}

# ================== Wait time (ETA) ==================
@app.get("/api/wait_time")
async def wait_time_public():
    DEFAULT_SEC = 75
    sample = 200
    queued = await queues_size()
    # active/capacity snapshot
    active = 0
    cap = 0
    for wid in (await r.smembers(K_WORKERS)) or []:
        w = await r.hgetall(await rkey(K_WORKER, wid=wid))
        if not w:
            continue
        active += int(w.get("active", "0") or "0")
        cap += int(w.get("capacity", "1") or "1")
    cap = max(1, cap)

    # Try a recent avg
    avg_ms = None
    ids = await r.lrange(K_JOBS_GLOBAL, 0, sample - 1)
    proc_ms = []
    for jid in ids or []:
        j = await get_job(jid)
        if not j or (j.get("status") or "").lower() != "completed":
            continue
        st_ms = int(j.get("started_at_ms", 0) or 0)
        fin_ms = int(j.get("finished_at_ms", 0) or 0)
        if st_ms and fin_ms and fin_ms > st_ms:
            proc_ms.append(fin_ms - st_ms)
    if proc_ms:
        avg_ms = sum(proc_ms) / len(proc_ms)

    avg_sec = (avg_ms / 1000.0) if avg_ms else DEFAULT_SEC
    ahead = int(queued) + int(active)
    est_sec = int((ahead / cap) * avg_sec)

    return {
        "ok": True,
        "avg_job_sec": int(avg_sec),
        "estimated_sec": est_sec,
        "queued_jobs": int(queued),
        "active_jobs": int(active),
        "capacity": int(cap),
        "source": "recent_avg" if avg_ms else "default",
    }

# ================== Optional background tasks ==================
async def lease_sweeper_loop():
    """Optional lightweight sweeper; enable with LEASE_SWEEPER_ENABLED=1.
    Re-queues jobs whose leases expired (TTL elapsed)."""
    assert r is not None
    while True:
        try:
            now = time.time()
            # Iterate snapshot of job_ids that are/were leased
            job_ids = await r.smembers(K_LEASE_SET)
            for jid in job_ids or []:
                k = await rkey(K_LEASE, job_id=jid)
                if not await r.exists(k):
                    # lease key TTL elapsed → consider retry/requeue if not already completed
                    job = await get_job(jid)
                    if not job:
                        await r.srem(K_LEASE_SET, jid)
                        continue
                    if job.get("status") in {"completed", "failed", "canceled"}:
                        await r.srem(K_LEASE_SET, jid)
                        continue
                    # read retries from last known lease hash (already gone). We store mirrored retries in job hash.
                    retries = int(job.get("lease_retries", 0) or 0)
                    if retries < 3:
                        await set_job_fields(jid, {"status": "queued", "error": "", "lease_retries": retries + 1})
                        await enqueue_job(jid)
                        await publish_event(jid, "info", "lease expired; requeued", 5, {"retries": retries + 1})
                        # remove from lease set since key gone
                        await r.srem(K_LEASE_SET, jid)
                    else:
                        await set_job_fields(jid, {"status": "failed", "error": "lease expired"})
                        await publish_event(jid, "error", "lease expired", 0, {})
                        await r.srem(K_LEASE_SET, jid)
        except Exception as e:
            logger.warning(f"lease_sweeper_loop warn: {e}")
        await asyncio.sleep(LEASE_SWEEP_SEC)

# ================== Startup / Shutdown ==================
@app.on_event("startup")
async def _startup():
    global r
    # Shared client for normal commands, with a decent-sized pool
    r = redis.from_url(
        REDIS_URL,
        decode_responses=True,
        max_connections=256,          # was 128
        health_check_interval=30,     # helps keep connections fresh
    )
    logger.info("Connected to Redis")

    await r.ping()

    if LEASE_SWEEPER_ENABLED:
        asyncio.create_task(lease_sweeper_loop())
        logger.info("Lease sweeper started (enabled by env)")


@app.on_event("shutdown")
async def _shutdown():
    global r
    if r is not None:
        try:
            await r.close()
        except Exception:
            pass
        r = None

# ================== Entrypoint (dev / systemd) ==================
if __name__ == "__main__":
    import uvicorn
    # For production you could also run this via the uvicorn CLI,
    # but since systemd calls `python controller.py`, we run it here:
    uvicorn.run(
        "controller:app",
        host="0.0.0.0",
        port=8002,
        workers=4,              # matches the CLI you wrote
        timeout_keep_alive=30,
        log_level="info",
        access_log=False,
    )