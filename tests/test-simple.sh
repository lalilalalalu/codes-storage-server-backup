#!/bin/bash

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi

out_dir=test-simple-output

rm -rf "$out_dir-ser" "$out_dir-ser-big" "$out_dir-opt-big"

./tests/test-simple --codes-config="${srcdir}"/tests/conf/test-simple.conf --lp-io-dir="$out_dir"-ser

set -e
grep "write_bytes:300000" "$out_dir"-ser/lsm-category-all >/dev/null
grep "read_bytes:300000" "$out_dir"-ser/lsm-category-all >/dev/null
set +e

./tests/test-simple --codes-config="${srcdir}"/tests/conf/test-simple-big.conf --lp-io-dir="$out_dir"-ser-big

set -e
grep "write_bytes:3000000" "$out_dir"-ser-big/lsm-category-all >/dev/null
grep "read_bytes:3000000" "$out_dir"-ser-big/lsm-category-all >/dev/null
set +e

# TODO
mpirun -np 4 ./tests/test-simple --sync=3 --codes-config="${srcdir}"/tests/conf/test-simple-big.conf --lp-io-dir="$out_dir"-opt-big

set -e
grep "write_bytes:3000000" "$out_dir"-opt-big/lsm-category-all >/dev/null
grep "read_bytes:3000000" "$out_dir"-opt-big/lsm-category-all >/dev/null
set +e

rm -rf "$out_dir-ser" "$out_dir-ser-big" "$out_dir-opt-big"

# dragonfly's turn

rm -rf "$out_dir-dfly-ser" "$out_dir-dfly-opt"

set -e
./tests/test-simple --codes-config="${srcdir}"/tests/conf/test-dragonfly.conf --lp-io-dir="$out_dir"-dfly-ser

wr=$(grep "write_bytes:150000" "$out_dir-dfly-ser"/lsm-category-all | wc -l)
rd=$(grep "read_bytes:150000" "$out_dir-dfly-ser"/lsm-category-all | wc -l)
set +e
if [[ $wr != 264 || $rd != 264 ]] ; then
    echo "error: wrong byte counts for serial dragonfly"
fi

set -e
./tests/test-simple --sync=3 --codes-config="${srcdir}"/tests/conf/test-dragonfly.conf --lp-io-dir="$out_dir"-dfly-opt

wr=$(grep "write_bytes:150000" "$out_dir-dfly-opt"/lsm-category-all | wc -l)
rd=$(grep "read_bytes:150000" "$out_dir-dfly-opt"/lsm-category-all | wc -l)
set +e
if [[ $wr != 264 || $rd != 264 ]] ; then
    echo "error: wrong byte counts for optimistic dragonfly"
fi

rm -rf "$out_dir-dfly-ser" "$out_dir-dfly-opt"
