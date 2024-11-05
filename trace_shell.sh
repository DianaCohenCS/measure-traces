#!/bin/bash

SCRIPT_DIR="scripts/"
TRACES=("Chicago16Small" "Chicago1610Mil" "ny19A" "ny19B" "SJ14.small")
ID_LENS=(80 80 64 64 64)

for i in ${!TRACES[@]}
do
    for batch_size in 50 100 250 500 1000 2000 4000
    do
        go run ${SCRIPT_DIR}batch/trace_batch.go ${TRACES[$i]} ${batch_size} ${ID_LENS[$i]} &
    done
    go run ${SCRIPT_DIR}all/trace_all.go ${TRACES[$i]}
done
