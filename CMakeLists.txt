cmake_minimum_required(VERSION 3.5.1)
project(mw-open-mpi CXX)

add_executable(mw-open-mpi main.cc)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set_property(TARGET mw-open-mpi PROPERTY CXX_STANDARD 11)
set_property(TARGET mw-open-mpi PROPERTY CXX_STANDARD_REQUIRED ON)

find_package(OpenMP REQUIRED)
if (OPENMP_FOUND)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()
find_package(MPI REQUIRED)

include_directories(/usr/include)
include_directories(/usr/include/mpi)
include_directories(/usr/include/mpich)
include_directories(/usr/include/openmpi)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
target_link_libraries(${PROJECT_NAME} ${MPI_CXX_LIBRARIES})
target_link_libraries(${PROJECT_NAME} ${OpenMP_CXX_LIBRARIES})