DEBUG = 
C_OMP = -fopenmp 

CC=gcc-9
CXX=g++-9
CLANG=clang
CXXFLAGS = -march=native -mtune=native -ftree-vectorize -Ofast -std=c++11 -Wall $(DEBUG) -I /usr/local/include -lboost_date_time -lboost_thread -lpthread



LIBS = 

TARGET = parser

all: $(TARGET)

clean:
	rm -rf *.o $(TARGET)

parser.o:
	$(CXX) $(CXXFLAGS) $(C_OMP) -c parser.cc -o parser.o

parser: parser.o
	$(CXX) -o parser parser.o

