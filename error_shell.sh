#!/bin/bash

SCRIPT_DIR="scripts/"
#TRACES=("Chicago16Small" "Chicago1610Mil" "ny19A" "ny19B" "SJ14.small")
TRACES=("Chicago1610Mil" "ny19B" "SJ14.small")

for i in ${!TRACES[@]}
do
    #for batch_size in 50 100 250 500 1000 2000 4000
    for batch_size in 100 500 2000 4000
    do
        go run ${SCRIPT_DIR}error/est_err_batch.go ${TRACES[$i]} ${batch_size} &
    done
done
