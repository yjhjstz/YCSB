#! /bin/bash

logdir="target/logs"
driver="mongodb-async"

# export JAVA_HOME=

# Command to remove the previous run data but leave empty data files.
#compact="db.usertable.remove();printjson(db.runCommand({'compact':'usertable','force':true}));"
compact="db.dropDatabase();"

mkdir -p ${logdir}


for workload in $workloads ; do
  for connection in $connections ; do
    #
    # Remove all documents and compact (avoid re-disk allocation)."
    #
    mongo --eval "${compact}" ${mongohost}/ycsb >> /dev/null 2>&1 
  
    #
    # Load the data set.
    #
    now=$( date -u "+%F %H:%M:%S" )
    printf "%s: Load: %9s with %-13s thread=%s conns=%4s" "${now}" ${workload} ${driver} ${threads} ${connection}
  
    let start=$( date -u "+%s" )
    ./bin/ycsb load ${driver} -s -P workloads/${workload} -threads ${threads}         \
             -p "mongodb.url=mongodb://${mongohost}/ycsb?safe=true&maxPoolSize=${connection}&waitQueueMultiple=1&streamtype=netty" \
             >  ${logdir}/${workload}-${driver}-threads_${threads}-conns_${connection}-load.out \
             2> ${logdir}/${workload}-${driver}-threads_${threads}-conns_${connection}-load.err
    let end=$( date -u "+%s" )
    let delta=end-start
    printf " --> %6d seconds\n" ${delta}
  
    #
    # Run the workload.
    #
    now=$( date -u "+%F %H:%M:%S" )
    printf "%s:  Run: %9s with %-13s thread=%s conns=%4s" "${now}" ${workload} ${driver} ${threads} ${connection}
  
    let start=$( date -u "+%s" )
    ./bin/ycsb run ${driver} -s -P workloads/${workload} -threads ${threads}             \
             -p "mongodb.url=mongodb://${mongohost}/ycsb?safe=true&maxPoolSize=${connection}&waitQueueMultiple=1&streamtype=netty" \
              >  ${logdir}/${workload}-${driver}-threads_${threads}-conns_${connection}-run.out \
              2> ${logdir}/${workload}-${driver}-threads_${threads}-conns_${connection}-run.err                  
    let end=$( date -u "+%s" )
    let delta=end-start
    printf " --> %6d seconds\n" ${delta}
  done
done
