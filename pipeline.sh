#!/bin/bash

set -e # Exit immediately if a command exits with a non-zero status.

echo "Starting pipeline..."

echo "========================================"
echo "[1/3] Running query_downcast.py"
echo "========================================"
python3 query_downcast.py

echo "========================================"
echo "[2/3] Running downcast_polars.py"
echo "========================================"
python3 downcast_polars.py

echo "========================================"
echo "[3/3] Running ensemble.py"
echo "========================================"
python3 ensemble.py

echo "========================================"
echo "Pipeline completed successfully!"
echo "========================================"
