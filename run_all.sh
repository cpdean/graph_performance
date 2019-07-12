#!/bin/bash
docker run --rm -v raw_data:/raw_data -e "DATA_DIR=/raw_data" graph_performance_redis_graph_199
