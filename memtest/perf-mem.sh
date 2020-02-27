sudo perf stat -B -e L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-loads-misses $1
