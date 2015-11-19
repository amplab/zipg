cmake_minimum_required(VERSION 2.8)
project(succinct-graph-core CXX)

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

# find_package(Threads) # necessary?

set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)
file(MAKE_DIRECTORY ${LIBRARY_OUTPUT_PATH})
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
file(MAKE_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})

set(INCLUDE include)
file(GLOB_RECURSE SOURCE_FILES src/*.cpp)
include_directories(${INCLUDE})
# Hacky: is there a better way?
include_directories(${PROJECT_SOURCE_DIR}/../external/succinct-cpp/core/include/)

add_library(succinctgraph STATIC ${SOURCE_FILES})
target_link_libraries(succinctgraph ${CMAKE_THREAD_LIBS_INIT})
set_target_properties(succinctgraph PROPERTIES LINKER_LANGUAGE CXX)

IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
   # Mac OS X specific code: has to have -mt
  target_link_libraries(succinctgraph succinct pthread boost_thread-mt boost_system-mt)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
IF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  # On EC2, can't have -mt
  target_link_libraries(succinctgraph succinct pthread boost_thread boost_system)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

add_executable(graph-partitioner src/partitioners.cpp)

add_executable(graph-encoder src/ThreadedGraphEncoder.cpp)
target_link_libraries(graph-encoder succinctgraph)

add_executable(partitioned-graph-formatter src/partitioned_graph_formatter.cc)
target_link_libraries(partitioned-graph-formatter succinctgraph)
