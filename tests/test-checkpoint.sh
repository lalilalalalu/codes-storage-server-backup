#!/bin/bash

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi

out_dir=test-checkpoint-output

rm -rf "$out_dir-ser" "$out_dir-ser-big" "$out_dir-opt-big"

#removing the data matching checks because we don't know how much data is going to be sent/received in this case


./tests/test-checkpoint --sync=1 --codes-conf="${srcdir}"/tests/conf/test-checkpoint.conf --lp-io-dir="$out_dir"-ser
mpirun -np 4 ./tests/test-checkpoint --sync=3 --codes-conf="${srcdir}"/tests/conf/test-checkpoint.conf --lp-io-dir="$out_dir"-ser
