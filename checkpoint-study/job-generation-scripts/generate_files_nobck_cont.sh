#!/bin/sh 
export BUILD_DIR=/gpfs/u/home/RSNT/RSNTmubm/barn/codes-2017/codes-storage-server/build
export CONF_DIR=/gpfs/u/home/RSNT/RSNTmubm/barn/codes-2017/codes-storage-server/tests/conf
export SYNCH=1
export NODES=1
export PROCS=1
export network_size=8316
export BATCH=2
export GVT=512
export NKPS=64

for alloc in 512 1024 2048 4096 8192
do 
	for dist in "cont-cons"
	do
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --random-bb=1 --rank-alloc-file=${dist}-alloc-8832-${alloc}.conf --workload-conf-file=workload-$alloc.conf --lp-io-dir=bb-model-adap-$alloc-random-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-1T.conf >& dragonfly-nobck-random-$network_size-$alloc-adap-1T-$dist.out" >& example-script-$alloc-adap-random-$dist-nb.sh
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --rank-alloc-file=${dist}-alloc-8832-${alloc}.conf --workload-conf-file=workload-$alloc.conf --lp-io-dir=bb-model-adap-$alloc-nearest-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-1T.conf >& dragonfly-nobck-nearest-$network_size-$alloc-adap-1T-$dist.out" >& example-script-$alloc-adap-nearest-$dist-nb.sh
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --random-bb=1 --rank-alloc-file=${dist}-alloc-8832-${alloc}.conf --workload-conf-file=workload-$alloc.conf --lp-io-dir=bb-model-minimal-$alloc-random-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-1T-minimal.conf >& dragonfly-nobck-random-$network_size-$alloc-minimal-1T-$dist.out" >& example-script-$alloc-min-random-$dist-nb.sh
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --rank-alloc-file=${dist}-alloc-8832-${alloc}.conf --workload-conf-file=workload-$alloc.conf --lp-io-dir=bb-model-minimal-$alloc-nearest-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-1T-minimal.conf >& dragonfly-nobck-nearest-$network_size-$alloc-minimal-1T-$dist.out" >& example-script-$alloc-min-nearest-$dist-nb.sh
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --random-bb=1 --rank-alloc-file=${dist}-alloc-8832-${alloc}.conf --workload-conf-file=workload-$alloc.conf --lp-io-dir=bb-model-prog-adap-$alloc-random-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-1T-prog-adap.conf >& dragonfly-nobck-random-$network_size-$alloc-pa-1T-$dist.out" >& example-script-$alloc-pa-random-$dist-nb.sh
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --rank-alloc-file=${dist}-alloc-8832-${alloc}.conf --workload-conf-file=workload-$alloc.conf --lp-io-dir=bb-model-pa-$alloc-nearest-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-1T-prog-adap.conf >& dragonfly-nobck-nearest-$network_size-$alloc-pa-1T-$dist.out" >& example-script-$alloc-pa-nearest-$dist-nb.sh
	done
done

