#bin/sh

make clean
make car_accident
make query
make utils
make -B parser.o
make -B parser
time ./parser
