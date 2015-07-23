#!/bin/bash

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi

out_dir=test-simple-output

rm -rf "$out_dir-ser" "$out_dir-ser-big" "$out_dir-opt-big"

./tests/test-simple --codes-config="${srcdir}"/tests/conf/test-simple.conf --lp-io-dir="$out_dir"-ser

set -e
grep "write_bytes:50000" "$out_dir"-ser/lsm-category-all >/dev/null
grep "read_bytes:50000" "$out_dir"-ser/lsm-category-all >/dev/null
set +e

./tests/test-simple --codes-config="${srcdir}"/tests/conf/test-simple-big.conf --lp-io-dir="$out_dir"-ser-big

set -e
grep "write_bytes:500000" "$out_dir"-ser-big/lsm-category-all >/dev/null
grep "read_bytes:500000" "$out_dir"-ser-big/lsm-category-all >/dev/null
set +e

# TODO
mpirun -np 4 ./tests/test-simple --sync=3 --codes-config="${srcdir}"/tests/conf/test-simple-big.conf --lp-io-dir="$out_dir"-opt-big

set -e
grep "write_bytes:500000" "$out_dir"-opt-big/lsm-category-all >/dev/null
grep "read_bytes:500000" "$out_dir"-opt-big/lsm-category-all >/dev/null
set +e

rm -rf "$out_dir-ser" "$out_dir-ser-big" "$out_dir-opt-big"
