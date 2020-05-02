cmake_minimum_required(VERSION 3.12)
project(mw-open-mpi CXX)

add_executable(mw-open-mpi mw-open-mpi.cpp)

find_package(OpenMP REQUIRED)
target_link_libraries(sample PRIVATE OpenMP::OpenMP_CXX)