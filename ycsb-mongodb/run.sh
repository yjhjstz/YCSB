#! /bin/bash
#export connections="0032 0064 0128 0256 0512 1024 2048 4096 8192"
export connections="0064 0128 "
export threads=24
export workloads=${workload:-"workloada"}
#export workloads="workloada workloadb workloadc"
export mongohost=${host:-"10.101.72.137:3001"}
# run  workload benchmark
sh run-benchmark.sh
# collect result to result dir
sh merge-results.sh
# use plot to generate png
(
   cd result
   sh plot.sh
)

zip -r ycsb-mongo-logs.zip target/*
rm -rf target/