cmake_minimum_required(VERSION 3.9)

project(cql-proxy VERSION 0.1 DESCRIPTION "CQL native protocol proxy")

add_executable(cql-proxy src/proxy.c)

target_include_directories(cql-proxy PRIVATE include)
target_include_directories(cql-proxy PRIVATE src)

target_link_libraries(cql-proxy uv cassandra)
