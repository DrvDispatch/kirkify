import os
import re
import json
import time
import threading
import logging
from logging.handlers import RotatingFileHandler
from uuid import uuid4
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, AsyncGenerator
from collections import deque

import requests
from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Depends, Query, Form
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --- Admin auth (JWT) ---
from jose import jwt, JWTError
from passlib.hash import bcrypt

# --- Firebase Admin ---
import firebase_admin
from firebase_admin import credentials, firestore, storage

# RTDB is optional (only if FIREBASE_DATABASE_URL is provided)
try:
    from firebase_admin import db as rtdb  # type: ignore
except Exception:
    rtdb = None  # noqa: N816

# --- Env / Config ---
from dotenv import load_dotenv

# Allow alternate env file for testing
load_dotenv(os.getenv("CONTROLLER_ENV_FILE", "/srv/kirk-controller/.env"))

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


# ========= Vast.ai HTTP API client (inventory only) =========
class VastClient:
    """
    Minimal Vast.ai HTTP API client used ONLY for discovery/inventory, not lifecycle control.
      GET /api/v0/instances/
      GET /api/v0/instances/{id}/
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        verify_ssl: bool = True,
        timeout: float = 6.0,
        backoff_429: float = 2.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.backoff_429 = backoff_429

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h

    def _req(self, method: str, path: str, params=None) -> Optional[dict]:
        url = f"{self.base_url}{path}"
        try:
            r = requests.request(
                method=method.upper(),
                url=url,
                headers=self._headers(),
                params=params or {},
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            if r.status_code == 429:
                time.sleep(self.backoff_429)
                r = requests.request(
                    method=method.upper(),
                    url=url,
                    headers=self._headers(),
                    params=params or {},
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )
            if not r.ok:
                return None
            return r.json()
        except Exception as e:
            logger.warning(f"VastClient req error: {e}")
            return None

    @staticmethod
    def _normalize_instances(payload: Optional[dict]) -> List[dict]:
        if not isinstance(payload, dict) or "instances" not in payload:
            return []
        data = payload["instances"]
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            if "id" in data:
                return [data]
            return [v for v in data.values() if isinstance(v, dict)]
        return []

    def list_instances(self) -> List[dict]:
        p = self._req("GET", "/instances/")
        return self._normalize_instances(p)

    def show_instance(self, inst_id: int) -> Optional[dict]:
        p = self._req("GET", f"/instances/{inst_id}/")
        arr = self._normalize_instances(p)
        return arr[0] if arr else None


# ================== App & CORS ==================
app = FastAPI(title="Kirkify Controller (Pull-based, Firebase)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://admin.keyauth.eu",
        "https://kirkify.nl",
        "https://www.kirkify.nl",
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================== Config ==================
# Vast API (for inventory only)
VAST_API_BASE = os.getenv("VAST_API_BASE", "https://console.vast.ai/api/v0")
VAST_API_KEY = os.getenv("VAST_API_KEY")
VAST_VERIFY_SSL = os.getenv("VAST_VERIFY_SSL", "1") not in {
    "0",
    "false",
    "False",
    "no",
}
VAST_REFRESH_SEC = float(os.getenv("VAST_REFRESH_SEC", "20.0"))
VAST_BACKOFF_429 = float(os.getenv("VAST_BACKOFF_429", "2.0"))
VAST_HTTP_TIMEOUT = float(os.getenv("VAST_HTTP_TIMEOUT", "6.0"))

# Lease + queue
JOB_LEASE_TIMEOUT_SEC = float(os.getenv("JOB_LEASE_TIMEOUT_SEC", "180.0"))
LEASE_SWEEP_SEC = float(os.getenv("LEASE_SWEEP_SEC", "2.0"))
HEARTBEAT_STALE_SEC = float(os.getenv("HEARTBEAT_STALE_SEC", "30.0"))
TOTAL_JOB_TIMEOUT_SEC = int(os.getenv("TOTAL_JOB_TIMEOUT_SEC", "300"))

# Admin JWT
JWT_SECRET = os.getenv("JWT_SECRET", "change-me")
JWT_ISS = os.getenv("JWT_ISS", "kirkify-controller")
JWT_AUD = os.getenv("JWT_AUD", "admin")
JWT_EXP_MIN = int(os.getenv("JWT_EXP_MIN", "720"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS_HASH = os.getenv("ADMIN_PASS_HASH") or bcrypt.hash(
    os.getenv("ADMIN_PASS", "admin123")
)

# Firebase Admin
FIREBASE_CREDENTIALS_PATH = os.getenv(
    "FIREBASE_CREDENTIALS_PATH", "/srv/kirk-controller/FirebaseAuth.json"
)
FIREBASE_STORAGE_BUCKET = os.getenv("FIREBASE_STORAGE_BUCKET")
FIREBASE_DATABASE_URL = os.getenv("FIREBASE_DATABASE_URL")

if not firebase_admin._apps:
    cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
    project_id = cred.project_id
    init_opts: Dict[str, Any] = {
        "storageBucket": FIREBASE_STORAGE_BUCKET or f"{project_id}.appspot.com",
        "projectId": project_id,
    }
    if FIREBASE_DATABASE_URL:
        init_opts["databaseURL"] = FIREBASE_DATABASE_URL
    firebase_admin.initialize_app(cred, init_opts)

fs = firestore.client()
bucket = storage.bucket()
rtdb_root = (
    rtdb.reference("/") if (FIREBASE_DATABASE_URL and rtdb is not None) else None
)

# ================== State ==================
_state_lock = threading.Lock()
_queue_lock = threading.Lock()

# Two-level priority queue
_job_queue_p0: deque[str] = deque()  # high priority
_job_queue_p1: deque[str] = deque()  # normal

# Keep your existing structures:
_leases: Dict[str, Dict[str, Any]] = {}  # job_id -> {"worker_id": str, "deadline_ts": float, "retries": int}
_workers: Dict[str, Dict[str, Any]] = {}  # worker_id -> info

# vast inventory
_vast_inventory: Dict[str, Dict[str, Any]] = {}
_vast_ip_index: Dict[str, str] = {}  # public_ip -> instance_id

# Allow config; default to your IP
PRIORITY_IPS = {
    ip.strip()
    for ip in os.getenv("PRIORITY_IPS", "94.110.221.234").split(",")
    if ip.strip()
}


# ================== Utilities ==================
def _client_ip(request: Request) -> str:
    xf = request.headers.get("x-forwarded-for", "")
    if xf:
        return xf.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _json_dt(d: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in (d or {}).items():
        out[k] = v.isoformat() if isinstance(v, datetime) else v
    return out


def safe_filename(name: str) -> str:
    base = os.path.basename(name or "").strip() or "upload.bin"
    base = re.sub(r"[^\w.\-]+", "_", base)
    return base[:120]


def _remove_from_queue(job_id: str) -> bool:
    removed = False
    with _queue_lock:
        try:
            _job_queue_p0.remove(job_id)
            removed = True
        except ValueError:
            pass
        try:
            _job_queue_p1.remove(job_id)
            removed = True
        except ValueError:
            pass
    return removed


# ================== Firestore helpers ==================
def jobs_col():
    return fs.collection("jobs")


def events_col(job_id: str):
    return jobs_col().document(job_id).collection("events")


def controller_logs_col():
    return fs.collection("controller_logs")


def worker_logs_col():
    return fs.collection("worker_logs")


def storage_input_path(job_id: str, filename: str) -> str:
    return f"jobs/{job_id}/input/{safe_filename(filename)}"


def storage_output_path(job_id: str) -> str:
    return f"jobs/{job_id}/output/output.jpg"


def upload_bytes_to_gcs(path: str, blob_bytes: bytes, content_type: str) -> None:
    blob = bucket.blob(path)
    blob.upload_from_string(
        blob_bytes, content_type=content_type or "application/octet-stream"
    )


def download_bytes_from_gcs(path: str) -> bytes:
    return bucket.blob(path).download_as_bytes()


def sign_url(path: str, hours: int = 24) -> str:
    return bucket.blob(path).generate_signed_url(
        version="v4",
        expiration=timedelta(hours=hours),
        method="GET",
    )


def push_event(
    job_id: str,
    etype: str,
    message: str,
    progress: Optional[int] = None,
    extra: Optional[dict] = None,
):
    doc = {"ts": firestore.SERVER_TIMESTAMP, "type": etype, "message": message}
    if progress is not None:
        doc["progress"] = progress
    if extra:
        doc["data"] = extra
    try:
        events_col(job_id).add(doc)
    except Exception as e:
        logger.warning(f"push_event Firestore error: {e}")
    # Optional RTDB mirror
    try:
        if rtdb_root is not None:
            payload = {
                "ts": int(time.time() * 1000),
                "type": etype,
                "message": message,
                "progress": progress,
                "data": extra or {},
            }
            rtdb_root.child("jobs").child(job_id).child("events").push(payload)
    except Exception as e:
        logger.warning(f"push_event RTDB warn: {e}")


def set_job(job_id: str, data: Dict[str, Any]):
    data["updated_at"] = firestore.SERVER_TIMESTAMP
    jobs_col().document(job_id).set(data, merge=True)


def create_job(job: Dict[str, Any]):
    job["created_at"] = firestore.SERVER_TIMESTAMP
    job["updated_at"] = firestore.SERVER_TIMESTAMP
    jobs_col().document(job["id"]).set(job)


def log_to_firestore(level: str, msg: str):
    level = (level or "info").lower()
    getattr(logger, level, logger.info)(msg)
    try:
        controller_logs_col().add(
            {"ts": firestore.SERVER_TIMESTAMP, "level": level, "message": msg}
        )
    except Exception:
        pass


# ================== Auth helpers ==================
class LoginReq(BaseModel):
    username: str
    password: str


def make_jwt(sub: str) -> str:
    exp = datetime.utcnow() + timedelta(minutes=JWT_EXP_MIN)
    payload = {"sub": sub, "iss": JWT_ISS, "aud": JWT_AUD, "exp": exp}
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def _decode_jwt(token: str) -> dict:
    return jwt.decode(
        token, JWT_SECRET, algorithms=["HS256"], audience=JWT_AUD, issuer=JWT_ISS
    )


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


# ================== Job queue helpers ==================
def enqueue_job(job_id: str, priority: bool = False):
    with _queue_lock:
        if priority:
            _job_queue_p0.append(job_id)
        else:
            _job_queue_p1.append(job_id)


def dequeue_job() -> Optional[str]:
    with _queue_lock:
        if _job_queue_p0:
            return _job_queue_p0.popleft()
        if _job_queue_p1:
            return _job_queue_p1.popleft()
        return None


def queue_size() -> int:
    with _queue_lock:
        return len(_job_queue_p0) + len(_job_queue_p1)


# ================== API Models (workers) ==================
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


# ================== HTTP API ==================
@app.get("/api/health")
def health():
    return {"ok": True, "status": "alive"}


@app.get("/api/ping")
def ping():
    return {"pong": True, "ts": time.time()}


# ---- Auth ----
@app.post("/api/auth/login")
def login(req: LoginReq):
    # bcrypt verify at cost=12 should be fast (<100ms). If this is slow, it's network/CDN, not compute here.
    if req.username != ADMIN_USER or not bcrypt.verify(
        req.password, ADMIN_PASS_HASH
    ):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"ok": True, "token": make_jwt(req.username), "user": req.username}


@app.get("/api/auth/me")
def me(_: bool = Depends(require_admin)):
    return {"ok": True, "user": ADMIN_USER}


# ---- GPU status for frontend chip (pool summary) ----
@app.get("/api/gpu_status")
def gpu_status():
    now = time.time()
    with _state_lock:
        online = sum(
            1
            for w in _workers.values()
            if (now - w.get("last_seen_ts", 0.0) < HEARTBEAT_STALE_SEC)
        )
        active = sum(int(w.get("active", 0)) for w in _workers.values())
        total_capacity = sum(int(w.get("capacity", 1)) for w in _workers.values())
    queued = queue_size()
    status = "ready" if online > 0 else "off"
    return {
        "status": status,
        "workers_online": online,
        "active_jobs": active,
        "total_capacity": total_capacity,
        "queued_jobs": queued,
        "pool": "pull-based",
    }


# ---- Worker registration / leasing (pull-based) ----
@app.post("/api/worker/register")
def worker_register(req: WorkerRegisterReq, request: Request):
    wid = uuid4().hex
    now = time.time()
    info = {
        "id": wid,
        "name": req.name or f"worker-{wid[:6]}",
        "public_url": req.public_url,
        "capacity": max(1, int(req.capacity or 1)),
        "active": 0,
        "last_seen_ts": now,
        "first_seen_ts": now,
        "tags": req.tags or {},
        "gpu": req.gpu or {},
        "remote_ip": _client_ip(request),
        "vast_instance_id": None,
    }
    with _state_lock:
        _workers[wid] = info
    log_to_firestore(
        "info",
        f"worker registered: {info['name']} ({wid}) ip={info['remote_ip']}",
    )
    return {
        "ok": True,
        "worker_id": wid,
        "lease_endpoint": "/api/worker/lease",
        "result_endpoint": "/api/worker/result",
        "error_endpoint": "/api/worker/error",
        "heartbeat_interval_sec": HEARTBEAT_STALE_SEC // 2,
    }


@app.post("/api/worker/heartbeat")
def worker_heartbeat(req: WorkerHeartbeatReq):
    now = time.time()
    with _state_lock:
        w = _workers.get(req.worker_id)
        if not w:
            raise HTTPException(status_code=404, detail="unknown worker_id")
        w["last_seen_ts"] = now
        if req.metrics:
            w["gpu"] = req.metrics
    return {"ok": True}


@app.post("/api/worker/lease")
def worker_lease(req: WorkerLeaseReq, request: Request):
    now = time.time()
    wid = req.worker_id
    wants = max(0, int(req.wants or 0))

    with _state_lock:
        w = _workers.get(wid)
        if not w:
            raise HTTPException(status_code=404, detail="unknown worker_id")
        w["last_seen_ts"] = now
        w["active"] = int(req.active or 0)
        if req.gpu:
            w["gpu"] = req.gpu
        w["remote_ip"] = _client_ip(request)
        free_slots = max(
            0, int(w.get("capacity", 1)) - int(w.get("active", 0))
        )
        grant = min(wants, free_slots, 1)  # fairness: at most 1

    if grant <= 0:
        return {"ok": True, "lease": None, "wait_sec": 2}

    job_id = dequeue_job()
    if not job_id:
        return {"ok": True, "lease": None, "wait_sec": 2}

    jdoc = jobs_col().document(job_id).get()
    if not jdoc.exists:
        return {"ok": True, "lease": None, "wait_sec": 2}

    job = jdoc.to_dict() or {}
    filename = job.get("filename", "upload.jpg")
    input_path = job.get("input_path")
    if not input_path:
        set_job(job_id, {"status": "failed", "error": "missing input_path"})
        push_event(job_id, "error", "missing input_path")
        return {"ok": True, "lease": None, "wait_sec": 1}

    try:
        url = sign_url(input_path, hours=1)
    except Exception as e:
        set_job(job_id, {"status": "failed", "error": f"sign_url failed: {e}"})
        push_event(job_id, "error", "sign_url failed")
        return {"ok": True, "lease": None, "wait_sec": 1}

    with _state_lock:
        _workers[wid]["active"] = int(_workers[wid].get("active", 0)) + 1
        _leases[job_id] = {
            "worker_id": wid,
            "deadline_ts": time.time() + JOB_LEASE_TIMEOUT_SEC,
            "retries": int(_leases.get(job_id, {}).get("retries", 0)),
        }

    set_job(
        job_id,
        {
            "status": "processing",
            "started_at": firestore.SERVER_TIMESTAMP,
            "worker_id": wid,
        },
    )
    push_event(job_id, "state", "processing", 40)

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
    with _state_lock:
        lease = _leases.get(job_id)
        w = _workers.get(worker_id)
    if not lease or not w or lease.get("worker_id") != worker_id:
        raise HTTPException(status_code=400, detail="invalid lease or worker_id")

    try:
        blob_bytes = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"read upload failed: {e}")

    out_path = storage_output_path(job_id)
    try:
        upload_bytes_to_gcs(
            out_path, blob_bytes, file.content_type or "image/jpeg"
        )
    except Exception as e:
        with _state_lock:
            _workers[worker_id]["active"] = max(
                0, int(_workers[worker_id].get("active", 0)) - 1
            )
            _leases.pop(job_id, None)
        set_job(
            job_id,
            {"status": "failed", "error": f"output upload error: {e}"},
        )
        push_event(job_id, "error", f"upload output error: {e}")
        return {"ok": False, "error": "upload_failed"}

    set_job(
        job_id,
        {
            "status": "completed",
            "output_path": out_path,
            "finished_at": firestore.SERVER_TIMESTAMP,
        },
    )
    push_event(
        job_id, "done", "completed", 100, {"output_path": out_path}
    )

    with _state_lock:
        _workers[worker_id]["active"] = max(
            0, int(_workers[worker_id].get("active", 0)) - 1
        )
        _leases.pop(job_id, None)

    return {"ok": True}


class WorkerErrorReq(BaseModel):
    worker_id: str
    job_id: str
    error: str


@app.post("/api/worker/error")
def worker_error(req: WorkerErrorReq):
    with _state_lock:
        lease = _leases.get(req.job_id)
        w = _workers.get(req.worker_id)
    if not w:
        raise HTTPException(status_code=404, detail="unknown worker")
    with _state_lock:
        _workers[req.worker_id]["active"] = max(
            0, int(_workers[req.worker_id].get("active", 0)) - 1
        )
        _leases.pop(req.job_id, None)
    retries = int(lease.get("retries", 0)) if lease else 0
    if retries < 3:
        set_job(req.job_id, {"status": "queued", "error": None})
        with _state_lock:
            _leases[req.job_id] = {
                "worker_id": None,
                "deadline_ts": 0.0,
                "retries": retries + 1,
            }
        enqueue_job(req.job_id)
        push_event(
            req.job_id,
            "info",
            f"requeued after error: {req.error}",
            5,
            {"retries": retries + 1},
        )
    else:
        set_job(req.job_id, {"status": "failed", "error": req.error})
        push_event(req.job_id, "error", req.error)
    return {"ok": True}


# ================== Jobs API (public + admin) ==================
class JobCreateResponse(BaseModel):
    id: str
    status: str


async def _create_job_common(request: Request, upload: UploadFile) -> Dict[str, Any]:
    client_id = request.headers.get("x-client-id") or request.query_params.get("client_id")
    user_agent = request.headers.get("user-agent") or ""
    caller_ip = _client_ip(request)
    is_prio = caller_ip in PRIORITY_IPS

    try:
        payload = await upload.read()
    except Exception as e:
        log_to_firestore("error", f"read upload failed: {e}")
        raise HTTPException(status_code=400, detail=f"read upload failed: {e}")

    filename = safe_filename(upload.filename or "upload")
    job_id = uuid4().hex
    inp_path = storage_input_path(job_id, filename)

    try:
        upload_bytes_to_gcs(inp_path, payload, upload.content_type or "application/octet-stream")
    except Exception as e:
        log_to_firestore("error", f"GCS upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"GCS upload failed: {e}")

    job = {
        "id": job_id,
        "status": "queued",
        "requested_by_ip": caller_ip,
        "client_id": client_id,
        "user_agent": user_agent,
        "filename": filename,
        "input_path": inp_path,
        "output_path": None,
        "events": [],
        "created_at": firestore.SERVER_TIMESTAMP,
        "started_at": None,
        "finished_at": None,
        "processing_ms": None,
        "mode": "async",
        "error": None,
    }
    create_job(job)

    with _queue_lock:
        active_now = sum(int(w.get("active", 0)) for w in _workers.values())
        p0_len = len(_job_queue_p0)
        p1_len = len(_job_queue_p1)
        # For priority jobs, your position ignores normal queue.
        # For normal jobs, you wait behind P0 + P1.
        position = (p0_len + active_now + 1) if is_prio else (p0_len + p1_len + active_now + 1)
        if is_prio:
            _job_queue_p0.append(job_id)
        else:
            _job_queue_p1.append(job_id)

    with _state_lock:
        cap = max(1, sum(int(w.get("capacity", 1)) for w in _workers.values()))

    push_event(
        job_id,
        "info",
        "job queued",
        1,
        {"filename": filename, "queue_position": position, "capacity": cap, "priority": is_prio},
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
def list_jobs(
    status: Optional[str] = Query(None),
    q: Optional[str] = Query(
        None, description="substring match on filename or IP"
    ),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _: bool = Depends(require_admin),
):
    query = (
        jobs_col()
        .order_by("created_at", direction=firestore.Query.DESCENDING)
        .offset(offset)
        .limit(500)
    )
    docs = [d.to_dict() or {} for d in query.stream()]
    items: List[Dict[str, Any]] = []
    for d in docs:
        if status and d.get("status") != status:
            continue
        if q:
            hay = f"{d.get('filename','')} {d.get('requested_by_ip','')}".lower()
            if q.lower() not in hay:
                continue
        items.append(_json_dt(d))
        if len(items) >= limit:
            break
    return {"ok": True, "items": items}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, _: bool = Depends(require_admin)):
    d = jobs_col().document(job_id).get()
    if not d.exists:
        raise HTTPException(status_code=404, detail="Not found")
    job = _json_dt(d.to_dict() or {})

    evs: List[Dict[str, Any]] = []
    for e in (
        events_col(job_id)
        .order_by("ts", direction=firestore.Query.DESCENDING)
        .limit(200)
        .stream()
    ):
        de = e.to_dict() or {}
        if isinstance(de.get("ts"), datetime):
            de["ts"] = de["ts"].isoformat()
        evs.append(de)
    evs.reverse()
    return {"ok": True, "job": job, "events": evs}


# --- Admin signed URL for images ---
@app.get("/api/jobs/{job_id}/signed_url")
def admin_job_signed_url(
    job_id: str, kind: str = Query("input"), _: bool = Depends(require_admin)
):
    doc = jobs_col().document(job_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Not found")
    j = doc.to_dict() or {}
    path = j.get("input_path" if kind == "input" else "output_path")
    if not path:
        raise HTTPException(status_code=404, detail="no_path")
    try:
        url = sign_url(path, hours=24)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"sign_url failed: {e}")
    return {"ok": True, "url": url}


# --- Admin: cancel / retry / delete (basic) ---
@app.post("/api/jobs/{job_id}/cancel")
def job_cancel(job_id: str, _: bool = Depends(require_admin)):
    doc = jobs_col().document(job_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Not found")
    j = doc.to_dict() or {}
    prev = j.get("status")
    with _state_lock:
        lease = _leases.pop(job_id, None)
        if lease and lease.get("worker_id") in _workers:
            wid = lease["worker_id"]
            _workers[wid]["active"] = max(
                0, int(_workers[wid].get("active", 0)) - 1
            )
    _remove_from_queue(job_id)
    set_job(job_id, {"status": "canceled"})
    push_event(
        job_id, "state", "canceled", 0, {"prev_status": prev}
    )
    return {"ok": True, "message": "canceled"}


@app.post("/api/jobs/{job_id}/retry")
def job_retry(job_id: str, _: bool = Depends(require_admin)):
    doc = jobs_col().document(job_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Not found")
    j = doc.to_dict() or {}
    if not j.get("input_path"):
        raise HTTPException(status_code=400, detail="no input_path")

    new_id = uuid4().hex
    new_job = {
        "id": new_id,
        "status": "queued",
        "requested_by_ip": j.get("requested_by_ip"),
        "client_id": j.get("client_id"),
        "user_agent": j.get("user_agent"),
        "filename": j.get("filename"),
        "input_path": j.get("input_path"),
        "output_path": None,
        "events": [],
        "created_at": firestore.SERVER_TIMESTAMP,
        "started_at": None,
        "finished_at": None,
        "processing_ms": None,
        "mode": "async",
        "error": None,
    }
    create_job(new_job)
    is_prio = (j.get("requested_by_ip") or "") in PRIORITY_IPS
    enqueue_job(new_id, priority=is_prio)
    push_event(
        new_id,
        "info",
        "job queued (retry)",
        1,
        {"from_job": job_id, "priority": is_prio},
    )
    return {"ok": True, "new_job_id": new_id}


@app.delete("/api/jobs/{job_id}")
def job_delete(job_id: str, _: bool = Depends(require_admin)):
    doc = jobs_col().document(job_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Not found")
    j = doc.to_dict() or {}
    _remove_from_queue(job_id)
    # best-effort remove storage blobs
    try:
        if j.get("input_path"):
            bucket.blob(j["input_path"]).delete(if_generation_match=None)
        if j.get("output_path"):
            bucket.blob(j["output_path"]).delete(if_generation_match=None)
    except Exception:
        pass
    # delete events
    try:
        for e in events_col(job_id).stream():
            e.reference.delete()
    except Exception:
        pass
    jobs_col().document(job_id).delete()
    return {"ok": True}


# --- Public SSE (no auth) ---
@app.get("/api/jobs/{job_id}/events")
async def stream_events_public(job_id: str):
    async def gen():
        last_ts: Optional[datetime] = None
        yield b"retry: 1000\n\n"
        while True:
            doc = jobs_col().document(job_id).get()
            if not doc.exists:
                break
            job = doc.to_dict() or {}
            status = job.get("status", "")

            q = events_col(job_id)
            if last_ts:
                q = q.where("ts", ">", last_ts)
            q = q.order_by("ts").limit(50)

            terminal = {"completed", "failed", "timeout", "canceled"}
            for e in q.stream():
                d = e.to_dict() or {}
                ts = d.get("ts")
                if isinstance(ts, datetime):
                    last_ts = ts
                    d["ts"] = ts.isoformat()
                yield b"data: " + json.dumps(d).encode("utf-8") + b"\n\n"

            if status in terminal:
                if status == "completed" and job.get("output_path"):
                    try:
                        url = sign_url(job["output_path"], hours=24)
                        payload = {"type": "completed", "output_url": url}
                        yield b"data: " + json.dumps(payload).encode("utf-8") + b"\n\n"
                    except Exception:
                        pass
                break

            await asyncio_sleep(1.0)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# --- Admin SSE (token) ---
@app.get("/api/jobs/{job_id}/events/stream")
async def stream_events_admin(job_id: str, token: str = Query(...)):
    try:
        _decode_jwt(token)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    async def gen():
        last_ts: Optional[datetime] = None
        yield b"retry: 1000\n\n"
        while True:
            jdoc = jobs_col().document(job_id).get()
            if not jdoc.exists:
                break
            j = jdoc.to_dict() or {}
            status = j.get("status", "")

            q = events_col(job_id)
            if last_ts:
                q = q.where("ts", ">", last_ts)
            q = q.order_by("ts").limit(50)

            new_any = False
            for e in q.stream():
                d = e.to_dict() or {}
                ts = d.get("ts")
                if isinstance(ts, datetime):
                    last_ts = ts
                    d["ts"] = ts.isoformat()
                yield b"data: " + json.dumps(d).encode("utf-8") + b"\n\n"
                new_any = True

            terminal = {"completed", "failed", "timeout", "canceled"}
            if status in terminal and not new_any:
                break

            await asyncio_sleep(1.0)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


async def asyncio_sleep(sec: float):
    import asyncio

    await asyncio.sleep(sec)


# ================== Public “My jobs” (persistent) ==================
@app.get("/api/my/jobs")
def my_jobs(
    request: Request,
    status: Optional[str] = None,
    limit: int = Query(10, ge=1, le=100),
    client_id: Optional[str] = Query(None),
):
    cid = client_id or request.headers.get("X-Client-Id") or request.cookies.get(
        "kirk_cid"
    )
    ip = _client_ip(request)
    items: List[Dict[str, Any]] = []
    try:
        qs = (
            jobs_col()
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(400)
            .stream()
        )
        for d in qs:
            j = d.to_dict() or {}
            owns = False
            if cid and j.get("client_id") == cid:
                owns = True
            elif not cid and j.get("requested_by_ip") == ip:
                owns = True
            if not owns:
                continue
            if status and j.get("status") != status:
                continue
            items.append(_json_dt(j))
            if len(items) >= limit:
                break
    except Exception as e:
        logger.warning(f"/api/my/jobs error: {e}")
    return {"ok": True, "items": items}


@app.get("/api/my/signed_url")
def my_signed_url(
    request: Request, job_id: str, kind: str = "output", client_id: Optional[str] = Query(None)
):
    cid = client_id or request.headers.get("X-Client-Id") or request.cookies.get("kirk_cid")
    doc = jobs_col().document(job_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Not found")
    j = doc.to_dict() or {}
    caller_ip = _client_ip(request)

    owner_ok = (cid and j.get("client_id") == cid) or (j.get("requested_by_ip") == caller_ip)
    if not owner_ok:
        raise HTTPException(status_code=403, detail="not your job")

    path = j.get("input_path" if kind == "input" else "output_path")
    if not path:
        raise HTTPException(status_code=404, detail="no_path")
    try:
        url = sign_url(path, hours=24)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"sign_url failed: {e}")
    return {"ok": True, "url": url}


# ================== Admin: diagnostics & metrics ==================
@app.get("/api/workers")
def list_workers(_: bool = Depends(require_admin)):
    now = time.time()
    with _state_lock:
        items = []
        for w in _workers.values():
            d = dict(w)
            d["stale_sec"] = now - w.get("last_seen_ts", 0.0)
            items.append(d)
    return {"ok": True, "items": items}


@app.get("/api/vast/instances")
def vast_instances(_: bool = Depends(require_admin)):
    with _state_lock:
        inv = {k: dict(v) for k, v in _vast_inventory.items()}
    return {"ok": True, "instances": inv}


@app.get("/api/metrics")
def metrics(
    sample: int = Query(200, ge=10, le=1000), _: bool = Depends(require_admin)
):
    by_status: Dict[str, int] = {}
    total = 0
    proc_ms: List[float] = []
    try:
        qs = (
            jobs_col()
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(sample)
            .stream()
        )
        for d in qs:
            j = d.to_dict() or {}
            total += 1
            st = (j.get("status") or "").lower()
            by_status[st] = by_status.get(st, 0) + 1
            st_at = j.get("started_at")
            fin = j.get("finished_at")
            if isinstance(st_at, datetime) and isinstance(fin, datetime):
                proc_ms.append(
                    (fin - st_at).total_seconds() * 1000.0
                )
    except Exception as e:
        logger.warning(f"metrics() error: {e}")
    avg = sum(proc_ms) / len(proc_ms) if proc_ms else None
    return {
        "ok": True,
        "by_status": by_status,
        "total_sampled": total,
        "avg_processing_ms": avg,
    }


@app.get("/api/worker_logs")
def api_worker_logs(
    limit: int = Query(200, ge=10, le=1000), _: bool = Depends(require_admin)
):
    items: List[Dict[str, Any]] = []
    try:
        qs = (
            worker_logs_col()
            .order_by("ts", direction=firestore.Query.DESCENDING)
            .limit(limit)
            .stream()
        )
        for d in qs:
            row = d.to_dict() or {}
            ts = row.get("ts")
            if isinstance(ts, datetime):
                row["ts"] = ts.isoformat()
            items.append(row)
        items.reverse()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"worker_logs error: {e}"
        )
    return {"ok": True, "items": items}


# --- Admin: stubs for warm/start/stop (pull-based) ---
@app.post("/api/warm_gpu")
def warm_gpu(_: bool = Depends(require_admin)):
    return {
        "ok": True,
        "message": "Pull-based pool warms on worker startup; nothing to do.",
    }


@app.post("/api/manual_start")
def manual_start(_: bool = Depends(require_admin)):
    return {
        "ok": True,
        "message": "Start/stop is not applicable in pull-based mode.",
    }


@app.post("/api/manual_stop")
def manual_stop(_: bool = Depends(require_admin)):
    return {
        "ok": True,
        "message": "Start/stop is not applicable in pull-based mode.",
    }


# ================== Background threads ==================
def _lease_sweeper():
    while True:
        try:
            now = time.time()
            expired: List[str] = []
            with _state_lock:
                for jid, lease in list(_leases.items()):
                    if lease.get("deadline_ts", 0.0) and now > lease["deadline_ts"]:
                        expired.append(jid)

            for jid in expired:
                with _state_lock:
                    lease = _leases.pop(jid, None)
                if not lease:
                    continue
                wid = lease.get("worker_id")
                if wid and wid in _workers:
                    with _state_lock:
                        _workers[wid]["active"] = max(
                            0, int(_workers[wid].get("active", 0)) - 1
                        )

                retries = int(lease.get("retries", 0))
                if retries < 3:
                    with _state_lock:
                        _leases[jid] = {
                            "worker_id": None,
                            "deadline_ts": 0.0,
                            "retries": retries + 1,
                        }
                    set_job(jid, {"status": "queued", "error": None})
                    enqueue_job(jid)
                    push_event(
                        jid,
                        "info",
                        "lease expired; requeued",
                        5,
                        {"retries": retries + 1},
                    )
                else:
                    set_job(
                        jid,
                        {"status": "failed", "error": "lease expired"},
                    )
                    push_event(jid, "error", "lease expired")

        except Exception as e:
            log_to_firestore("error", f"lease_sweeper error: {e}")
        time.sleep(LEASE_SWEEP_SEC)


def _workers_gc():
    while True:
        try:
            # (kept for future; we retain stale records for admin)
            time.sleep(10.0)
        except Exception as e:
            log_to_firestore("error", f"workers_gc error: {e}")
            time.sleep(10.0)


def _vast_inventory_refresher():
    vc = VastClient(
        base_url=VAST_API_BASE,
        api_key=VAST_API_KEY,
        verify_ssl=VAST_VERIFY_SSL,
        timeout=VAST_HTTP_TIMEOUT,
        backoff_429=VAST_BACKOFF_429,
    )
    while True:
        try:
            items = vc.list_instances() or []
            ip_index: Dict[str, str] = {}
            inventory: Dict[str, Dict[str, Any]] = {}
            for inst in items:
                iid = str(inst.get("id") or "")
                if not iid:
                    continue
                inventory[iid] = inst
                ip = str(inst.get("public_ipaddr") or "").strip()
                if ip:
                    ip_index[ip] = iid

            with _state_lock:
                _vast_inventory.clear()
                _vast_inventory.update(inventory)
                _vast_ip_index.clear()
                _vast_ip_index.update(ip_index)

                for w in _workers.values():
                    rip = w.get("remote_ip")
                    if rip and rip in _vast_ip_index:
                        w["vast_instance_id"] = _vast_ip_index[rip]
        except Exception as e:
            log_to_firestore("warn", f"vast inventory refresh failed: {e}")
        time.sleep(VAST_REFRESH_SEC)


@app.get("/api/wait_time")
def wait_time_public():
    """
    Public ETA hint for the frontend (no auth):
      - Uses recent completed jobs to compute average when possible
      - Falls back to DEFAULT_SEC when sampling is unavailable
    """
    DEFAULT_SEC = 75
    sample = 200
    avg_ms = None
    queued = queue_size()
    with _state_lock:
        active = sum(int(w.get("active", 0)) for w in _workers.values())
        cap = max(1, sum(int(w.get("capacity", 1)) for w in _workers.values()))

    # Try to compute avg from recent jobs (best-effort)
    try:
        proc_ms = []
        qs = jobs_col().order_by("created_at", direction=firestore.Query.DESCENDING).limit(sample).stream()
        for d in qs:
            j = d.to_dict() or {}
            if (j.get("status") or "").lower() != "completed":
                continue
            st_at = j.get("started_at")
            fin = j.get("finished_at")
            if isinstance(st_at, datetime) and isinstance(fin, datetime):
                ms = (fin - st_at).total_seconds() * 1000.0
                if ms > 0:
                    proc_ms.append(ms)
        if proc_ms:
            avg_ms = sum(proc_ms) / len(proc_ms)
    except Exception:
        pass

    avg_sec = (avg_ms / 1000.0) if avg_ms else DEFAULT_SEC
    ahead = queued + active
    est_sec = int((ahead / cap) * avg_sec)

    return {
        "ok": True,
        "avg_job_sec": int(avg_sec),
        "estimated_sec": est_sec,
        "queued_jobs": queued,
        "active_jobs": active,
        "capacity": cap,
        "source": "recent_avg" if avg_ms else "default",
    }


# ================== Startup ==================
_lease_sweeper_thread: Optional[threading.Thread] = None
_workers_gc_thread: Optional[threading.Thread] = None
_vast_thread: Optional[threading.Thread] = None


@app.on_event("startup")
def _startup():
    global _lease_sweeper_thread, _workers_gc_thread, _vast_thread

    if _lease_sweeper_thread is None or not _lease_sweeper_thread.is_alive():
        _lease_sweeper_thread = threading.Thread(
            target=_lease_sweeper, daemon=True, name="lease-sweeper"
        )
        _lease_sweeper_thread.start()
        log_to_firestore("info", "lease sweeper started")

    if _workers_gc_thread is None or not _workers_gc_thread.is_alive():
        _workers_gc_thread = threading.Thread(
            target=_workers_gc, daemon=True, name="workers-gc"
        )
        _workers_gc_thread.start()
        log_to_firestore("info", "workers GC started")

    if _vast_thread is None or not _vast_thread.is_alive():
        _vast_thread = threading.Thread(
            target=_vast_inventory_refresher,
            daemon=True,
            name="vast-inventory",
        )
        _vast_thread.start()
        log_to_firestore("info", "vast inventory refresher started")

    log_to_firestore("info", "Controller server started (pull-based).")


if __name__ == "__main__":
    import uvicorn

    log_to_firestore("info", "Starting controller server…")
    uvicorn.run(
        "controller:app",
        host="0.0.0.0",
        port=8002,
        log_level="info",
        access_log=False,
    )