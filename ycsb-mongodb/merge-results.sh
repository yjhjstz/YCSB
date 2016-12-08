#! /bin/bash

echo "connection" > /tmp/connections
for t in $connections; do
    echo $t >> /tmp/connections
done

for w in $workloads; do
    echo "ops/sec" > /tmp/thp
    grep Throughput target/logs/${w}*-run.out | awk '{print $3}' | awk -F. '{print $1}' >> /tmp/thp
    paste /tmp/connections /tmp/thp > result/${w}_ops.dat
done

for w in $workloads; do
    echo avg-read > /tmp/avg_read
    grep "READ.*AverageLatency" target/logs/${w}*.out | awk '{print $3}' | awk -F. '{print $1}' >> /tmp/avg_read
    echo 95th-read > /tmp/95th_read
    grep "READ.*95thPercentileLatency" target/logs/${w}*.out | awk '{print $3}' | awk -F. '{print ($1 + 1) * 1000}' >> /tmp/95th_read
    echo 99th-read > /tmp/99th_read
    grep "READ.*99thPercentileLatency" target/logs/${w}*.out | awk '{print $3}' | awk -F. '{print ($1 + 1) * 1000}' >> /tmp/99th_read
    paste /tmp/connections /tmp/avg_read /tmp/95th_read /tmp/99th_read > result/${w}_read_latency.dat

    echo avg-update > /tmp/avg_update
    grep "UPDATE.*AverageLatency" target/logs/${w}*.out | awk '{print $3}' | awk -F. '{print $1}' >> /tmp/avg_update
    echo 95th-update > /tmp/95th_update
    grep "UPDATE.*95thPercentileLatency" target/logs/${w}*.out | awk '{print $3}' | awk -F. '{print ($1 + 1) * 1000}' >> /tmp/95th_update
    echo 99th-update > /tmp/99th_update
    grep "UPDATE.*99thPercentileLatency" target/logs/${w}*.out | awk '{print $3}' | awk -F. '{print ($1 + 1) * 1000}' >> /tmp/99th_update
    paste /tmp/connections /tmp/avg_update /tmp/95th_update /tmp/99th_update > result/${w}_update_latency.dat

done
