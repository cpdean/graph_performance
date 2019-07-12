#!/bin/bash
# start server in background for test
redis-server --loadmodule /usr/lib/redis/modules/redisgraph.so &
# set env vars for finding raw data and writing results
# DATA_DIR, OUTPUT_DIR
echo "gonna run a test"
python3 /test_scripts/performance.py
