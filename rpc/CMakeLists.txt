cmake_minimum_required(VERSION 2.8)
project(succinct-graph-rpc CXX)

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
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} $ENV{SGFLAGS} -O3 -g")

find_package(Thrift REQUIRED)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
file(MAKE_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})

set(SOURCES_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(CORE_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../core/include)

set(THRIFT_SERVICE_FILE ${CMAKE_CURRENT_SOURCE_DIR}/thrift/succinct_graph.thrift)
thrift_gen_cpp(${THRIFT_SERVICE_FILE} ${SOURCES_DIR} ${INCLUDE_DIR} HANDLER_SOURCES)

set(GENERAL_SOURCES ${SOURCES_DIR}/succinct_graph_constants.cpp
    ${SOURCES_DIR}/succinct_graph_types.cpp)

set(SERVER_SOURCES ${GENERAL_SOURCES}
    ${SOURCES_DIR}/GraphQueryService.cpp
    ${SOURCES_DIR}/GraphQueryServiceServer.cpp)

set(HANDLER_SOURCES ${GENERAL_SOURCES}
    ${SOURCES_DIR}/GraphQueryAggregator.cpp
    ${SOURCES_DIR}/GraphQueryAggregatorService.cpp)

set(CLIENT_SOURCES ${GENERAL_SOURCES}
    ${SOURCES_DIR}/GraphQueryAggregatorService.cpp
    ${SOURCES_DIR}/GraphQueryService.cpp)

include_directories(${INCLUDE_DIR} ${CORE_INCLUDE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/../external/succinct-cpp/core/include)

add_executable(graph_query_server ${SERVER_SOURCES})
add_executable(graph_query_aggregator ${HANDLER_SOURCES})
add_library(succinctgraph-client ${CLIENT_SOURCES})

target_link_libraries(graph_query_server succinctgraph)
target_link_libraries(graph_query_server ${THRIFT_LIBRARIES})
target_link_libraries(graph_query_aggregator succinctgraph succinctgraph-client ${THRIFT_LIBRARIES})
