#!/bin/bash

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi

out_dir=test-checkpoint-output

rm -rf "$out_dir-ser" "$out_dir-ser-big" "$out_dir-opt-big"

#removing the data matching checks because we don't know how much data is going to be sent/received in this case

./tests/test-checkpoint --workload_type = checkpoint_io_workload --codes-config="${srcdir}"/tests/conf/test-simple.conf --lp-io-dir="$out_dir"-ser
./tests/test-checkpoint --workload_type = checkpoint_io_workload --codes-config="${srcdir}"/tests/conf/test-simple-big.conf --lp-io-dir="$out_dir"-ser-big
mpirun -np 4 ./tests/test-checkpoint --sync=3 --codes-config="${srcdir}"/tests/conf/test-simple-big.conf --lp-io-dir="$out_dir"-opt-big
./tests/test-simple --workload_type = checkpoint_io_workload --sync=3 --codes-config="${srcdir}"/tests/conf/test-dragonfly.conf --lp-io-dir="$out_dir"-dfly-opt
