DEBUG = 
C_OMP = -fopenmp 

CC=gcc-9
CXX=g++-9
CLANG=clang
CXXFLAGS = -march=native -mtune=native -ftree-vectorize -Ofast -std=c++17 -Wall $(DEBUG) -I /usr/local/include -lboost_date_time -lboost_thread -lpthread
LDFLAGS='-fopenmp -o


LIBS = 

TARGET = parser

all: $(TARGET)

clean:
	rm -rf *.o $(TARGET)

car_accident:
	g++ -c CarAccident.cpp -o CarAccident.o

query:
	$(CXX) $(CXXFLAGS) $(C_OMP) -I . -c Query.cpp -o Query.o

utils:
	$(CXX) $(CXXFLAGS) $(C_OMP) -c Utils.cpp -o Utils.o

parser.o:
	$(CXX) $(CXXFLAGS) $(C_OMP) -I. -c parser.cc -o parser.o

parser: parser.o
	$(CXX) $(C_OMP) -o parser parser.o

