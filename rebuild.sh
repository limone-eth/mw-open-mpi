#bin/sh

make clean
make all
PMIX_MCA_gds=^ds21 mpirun -hostfile /home/hostfile /home/mpi -n 2 ./mw-open-mpi
