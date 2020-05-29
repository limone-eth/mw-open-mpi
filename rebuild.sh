#bin/sh

make clean
make car_accident
make query
make utils
make -B main.o
make -B main
time ./main
