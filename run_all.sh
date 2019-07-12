#!/bin/bash
docker run \
    --rm \
    --mount "type=bind,src=$(pwd)/raw_data,dst=/raw_data,readonly" \
    -e "DATA_DIR=/raw_data" \
    graph_performance_redis_graph_199
