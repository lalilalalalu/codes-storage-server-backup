#!/bin/sh 
export BUILD_DIR=/gpfs/u/home/RSNT/RSNTmubm/barn/codes_2016/codes-storage-server/build
export CONF_DIR=/gpfs/u/home/RSNT/RSNTmubm/barn/codes_2016/codes-storage-server/config
export SYNCH=1
export NODES=1
export PROCS=1
export network_size=8232
export BATCH=2
export GVT=512
export NKPS=64

for alloc in 512 1024 2048 4096 8192
do 
	for dist in "grop" "rotr" "chassis" "node"
	do
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --rank-alloc-file=$CONF_DIR/rand_${dist}0-alloc-8316-$alloc.conf --workload-conf-file=$CONF_DIR/workload-$alloc.conf --lp-io-dir=bb-model-9K-$alloc-nearest-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-9K-adaptive-1T.conf >& dragonfly-nobck-nearest-$network_size-$alloc-adaptive-1T-$PROCS-$BATCH-$GVT-$NKPS-$dist.out" >& example-script-$alloc-nearest-$dist.sh
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --random-bb=1 --rank-alloc-file=$CONF_DIR/rand_${dist}0-alloc-8316-$alloc.conf --workload-conf-file=$CONF_DIR/workload-$alloc.conf --lp-io-dir=bb-model-9K-$alloc-random-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-9K-adaptive-1T.conf >& dragonfly-nobck-random-$network_size-$alloc-adaptive-1T-$PROCS-$BATCH-$GVT-$NKPS-$dist.out" >& example-script-$alloc-random-$dist.sh
	done
done

