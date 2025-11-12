#!/bin/bash
set -e

nohup entrypoint.sh &

# --- Cloudflared tunnel (gpu.keyauth.eu) ---
if command -v cloudflared >/dev/null 2>&1; then
  nohup bash -c '
  while true; do
    echo "[cloudflared] starting tunnel gpu-worker"
    cloudflared --config /root/.cloudflared/config.yml tunnel run gpu-worker >> /root/cloudflared.log 2>&1
    echo "[cloudflared] crashed, restarting in 5s"
    sleep 5
  done
  ' &
else
  echo "[onstart] cloudflared not found — skipping tunnel"
fi

# --- GPU worker ---
cd /workspace/gpu_worker
if [ -f "venv/bin/activate" ]; then
  source venv/bin/activate
  nohup bash -c '
  while true; do
    echo "[worker] starting uvicorn"
    uvicorn worker:app --host 0.0.0.0 --port 8002 >> /workspace/gpu_worker/worker.log 2>&1
    echo "[worker] crashed, restarting in 5s"
    sleep 5
  done
  ' &
  deactivate
else
  echo "[onstart] GPU worker venv not found — skipping launch"
fi

echo "[onstart] all background processes launched"
