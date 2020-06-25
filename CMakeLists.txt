cmake_minimum_required(VERSION 3.5.1)
project(mw-open-mpi)

add_executable(mw-open-mpi main.cc)

find_package(OpenMP REQUIRED)
find_package(MPI REQUIRED)
set_property(TARGET mw-open-mpi PROPERTY CXX_STANDARD 11)
set_property(TARGET mw-open-mpi PROPERTY CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")
include_directories(/usr/local/include)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
target_link_libraries(${PROJECT_NAME} ${MPI_CXX_LIBRARIES})
target_link_libraries(${PROJECT_NAME} ${OpenMP_CXX_LIBRARIES})