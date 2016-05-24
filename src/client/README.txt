The client-mul-wrklds takes the allocation file and the workload description
and maps the workloads onto the simulated ranks as provided by the allocation file. 

With the current experimental setup, we have a 1,056 node dragonfly network in
which there are 264 routers. Each router has 4 terminals/endpoints connected to it. 
The first node connected to each router is a storage or burst buffer node, the rest of
the three nodes are compute nodes. Among the 792 compute nodes (Overall, there are 264
storage nodes and 792 compute nodes) that are connected to the routers, there
are two types of workloads running on these compute nodes. 396 of the compute
nodes are running a checkpoint restart workload that generates a burst of
checkpoint traffic directed to the storage/burst buffer node connected to it.
Rest of the 396 compute nodes are generating uniform random traffic that is
intended to a randomly chose compute node from these 396 nodes (this means that
the compute nodes generating checkpoint restart traffic will not get any
uniform random traffic, the workloads are totally disjoint). 

Out of the 792 compute nodes, both workloads are assigned to nodes on a random
basis. So within a dragonfly group, there could be a mix of compute nodes that
will generate uniform random traffic and checkpoint restart traffic.

The allocation.conf file has the set of node IDs that will generate the checkpoint
restart and uniform random workloads. The workloads-1K-bb.conf has the
information on which set of nodes will be assigned what workload. In this
specific case, the first set of nodes in the allocation.conf file will generate
synthetic traffic and the second set will generate checkpoint restart traffic.

There has been an update to the dragonfly statistics as well. We now want to
keep track of how much traffic came from each of the workload on the terminals
and the routers so we now have category IDs in the data file. Each
router/terminal entry in the data file is keeping track of both workloads now.

The busy time of terminal is not on a category basis because its hard to keep track of
which packets cause the buffer overflows.

There is a category ID (hashed value) that identified the workload type. 

Hash value: 4143613225 Workload: Uniform random traffic
Hash value: 181202707  Workload: Checkpoint restart workload

Mean interval between uniform random messages: up to 10ms.
Each uniform random message size : up to 2 KiB
Checkpoint restart aggregate data transfer 50 GiB for 396 ranks.

Note: This simulation is a work in progress right now. 
