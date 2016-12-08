gnuplot -c workloada_ops.dem
gnuplot -c workloada_read_latency.dem
gnuplot -c workloada_update_latency.dem
gnuplot -c workloadb_ops.dem
gnuplot -c workloadb_read_latency.dem
gnuplot -c workloadb_update_latency.dem
gnuplot -c workloadc_ops.dem
gnuplot -c workloadc_read_latency.dem
#gnuplot -c workloadc_update_latency.dem

mkdir image 2>/dev/null
mv *.png image
