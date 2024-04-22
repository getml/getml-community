#!/bin/sh
cd /home/getml/engine
./getML --install --home-directory=/home/getml/storage &

cd /home/getml/benchmarks
python3.8 run_benchmarks.py