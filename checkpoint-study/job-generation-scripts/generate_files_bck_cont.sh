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

for alloc in 512
do 
	for dist in "cont"
	do
		let rem=8000
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --random-bb=1 --rank-alloc-file=${dist}-alloc-8832-${alloc}_${rem}.conf --workload-conf-file=workload-$alloc-$rem.conf --lp-io-dir=adap-routing-new/background/random-bb/$dist/bb-model-adap-$alloc-random-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-1T.conf >& adap-routing-new/background/random-bb/$dist/dragonfly-bck-random-$network_size-$alloc-adap-1T-$dist.out" >& example-script-$alloc-adap-random-$dist-bck.sh
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --rank-alloc-file=${dist}-alloc-8832-${alloc}_${rem}.conf --workload-conf-file=workload-$alloc-$rem.conf --lp-io-dir=adap-routing-new/background/nearest/$dist/bb-model-adap-$alloc-nearest-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-1T.conf >& adap-routing-new/background/nearest/$dist/dragonfly-bck-nearest-$network_size-$alloc-adap-1T-$dist.out" >& example-script-$alloc-adap-nearest-$dist-bck.sh
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --random-bb=1 --rank-alloc-file=${dist}-alloc-8832-${alloc}.conf --workload-conf-file=workload-$alloc.conf --lp-io-dir=adap-routing-new/no-background/random-bb/$dist/bb-model-adap-$alloc-random-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-1T.conf >& adap-routing-new/background/random-bb/$dist/dragonfly-bck-random-$network_size-$alloc-adap-1T-$dist.out" >& example-script-$alloc-adap-random-$dist-nobck.sh
		printf "#!/bin/sh \nsrun --ntasks-per-node=$PROCS --nodes=$NODES $BUILD_DIR/src/client/client-mul-wklds --extramem=20310720 --sync=1 --rank-alloc-file=${dist}-alloc-8832-${alloc}.conf --workload-conf-file=workload-$alloc.conf --lp-io-dir=adap-routing-new/no-background/nearest/$dist/bb-model-adap-$alloc-nearest-$dist --lp-io-use-suffix=1 --codes-config=$CONF_DIR/test-checkpoint-dfly-1T.conf >& adap-routing-new/background/nearest/$dist/dragonfly-bck-nearest-$network_size-$alloc-adap-1T-$dist.out" >& example-script-$alloc-adap-nearest-$dist-nobck.sh
	done
done

