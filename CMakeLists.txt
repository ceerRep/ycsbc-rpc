project(YCSBC-Seastar-RPC)
cmake_minimum_required(VERSION 2.8)

set (CMAKE_CXX_STANDARD 17)

find_package (Seastar REQUIRED)
find_package (Threads REQUIRED)
find_package (Boost REQUIRED serialization)

include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/thirdparty/ycsbc
    ${Boost_INCLUDE_DIRS}
)

add_subdirectory(thirdparty/ycsbc)

add_executable(rpc-server rpc-server.cc)
target_link_libraries(rpc-server Seastar::seastar ${CMAKE_THREAD_LIBS_INIT} Boost::serialization)

add_executable(rpc-client rpc-client.cc)
target_link_libraries(rpc-client Seastar::seastar ${CMAKE_THREAD_LIBS_INIT} Boost::serialization ycsb)
