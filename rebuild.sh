#bin/sh

make clean
make parser.o
make parser
time ./parser
