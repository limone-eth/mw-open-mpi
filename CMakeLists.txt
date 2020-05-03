cmake_minimum_required(VERSION 3.12)
project(mw-open-mpi CXX)

add_executable(mw-open-mpi parser.cc)

find_package(OpenMP REQUIRED)
include_directories(/usr/local/include)
target_link_libraries(OpenMP::OpenMP_CXX)