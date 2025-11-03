#!/bin/bash
set -e
echo "Installing dependencies..."
pip install --no-cache-dir -r requirements.txt
echo "Starting FastAPI server..."
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
