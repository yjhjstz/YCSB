#! /bin/bash

#export connections="0004 0008 0016 0032 0064 0128 0256 0512 1024 2048 4096 8192"
export connections="0004 0008 0016 0032 0064"
export workloads="workloada workloadb workloadc"
export threads=8
export mongohost="127.0.0.1:27017"
# run  workload benchmark
sh run-benchmark.sh
# collect result to result dir
sh merge-results.sh
# use plot to generate png
(
   cd result
   sh plot.sh
)

