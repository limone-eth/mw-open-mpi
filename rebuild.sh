#bin/sh

make clean
make -B parser.o
make -B parser
time ./parser
