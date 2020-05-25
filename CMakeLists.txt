cmake_minimum_required(VERSION 3.12)
project(mw-open-mpi)

add_executable(mw-open-mpi parser.cc CarAccident.cpp CarAccident.h Query.cpp Query.h Utils.cpp Utils.h)

find_package(OpenMP REQUIRED)
include_directories(/usr/local/include)
target_link_libraries(${PROJECT_NAME} ${OpenMP_CXX_LIBRARIES})