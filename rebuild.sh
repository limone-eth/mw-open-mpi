#bin/sh

make clean
make all
PMIX_MCA_gds=^ds21 mpirun -hostfile /home/mpich2.hosts -n 2 ./mw-open-mpi
mpirun -host master,master,master,master -n 4 --allow-run-as-root -mca btl ^openib --oversubscribe -use-hwthread-cpus ./mw-open-mpi-exec