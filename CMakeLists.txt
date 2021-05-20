cmake_minimum_required(VERSION 3.9)

project(cql-proxy VERSION 0.1 DESCRIPTION "CQL native protocol proxy")

find_package(ZLIB REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

find_package(PkgConfig REQUIRED)
pkg_check_modules (JEMALLOC jemalloc)

add_executable(cql-proxy src/proxy.c)

target_include_directories(cql-proxy PRIVATE include)
target_include_directories(cql-proxy PRIVATE src)

target_link_libraries(cql-proxy cassandra_static uv ZLIB::ZLIB OpenSSL::SSL OpenSSL::Crypto Threads::Threads stdc++ m ${JEMALLOC_LIBRARIES})
