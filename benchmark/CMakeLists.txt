cmake_minimum_required(VERSION 2.8)
project(succinct-graph-bench CXX)

enable_language(C)
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if(COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)
    if(COMPILER_SUPPORTS_CXX0X)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
    else()
        message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
    endif()
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} $ENV{SGFLAGS} -O3 -Wall -g")

find_package(Thrift REQUIRED)
# find_package(Threads) # necessary?

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
file(MAKE_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})

set(INCLUDE include ../core/include ../external/succinct-cpp/core/include ../rpc/include)
include_directories(${INCLUDE})

file(GLOB_RECURSE SOURCE_FILES src/*.cpp)

add_executable(bench src/succinct-bench.cpp)
target_link_libraries(bench succinctgraph)
target_link_libraries(bench succinctgraph-client)
target_link_libraries(bench ${THRIFT_LIBRARIES})

add_executable(create src/succinct-create.cpp)
target_link_libraries(create succinctgraph)
target_link_libraries(create succinctgraph-client)
target_link_libraries(create ${THRIFT_LIBRARIES})

add_executable(tests src/tests.cpp)
target_link_libraries(tests succinctgraph)
