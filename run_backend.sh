#!/bin/bash

# Navigate to the backend directory
cd "$(dirname "$0")"

# Kill any existing processes on port 8000
echo "Cleaning up any processes on port 8000..."
kill -9 $(lsof -t -i:8000) 2>/dev/null || true

# Start uvicorn backend
echo "Starting FastAPI backend on port 8000..."
./venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000 &
BACKEND_PID=$!

# Wait for backend to spin up
sleep 2

# Start localtunnel with auto-reconnect loop
echo "Starting localtunnel auto-reconnect loop for subdomain 'webzoagency-api'..."
while true; do
  npx localtunnel --port 8000 --local-host 127.0.0.1 --subdomain webzoagency-api
  echo "Localtunnel disconnected. Reconnecting in 5 seconds..."
  sleep 5
done &
TUNNEL_PID=$!

# Handle shutdown gracefully
cleanup() {
  echo "Shutting down backend and localtunnel..."
  kill -9 $BACKEND_PID 2>/dev/null || true
  kill -9 $TUNNEL_PID 2>/dev/null || true
  exit 0
}

trap cleanup INT TERM EXIT

# Keep script running
wait $BACKEND_PID $TUNNEL_PID
