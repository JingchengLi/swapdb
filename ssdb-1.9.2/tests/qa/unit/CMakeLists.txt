cmake_minimum_required(VERSION 2.8)
project(ssdb-test)

option(ROCKSDB "enable rocksdb support" ON)
SET(BUILD_PATH "${CMAKE_SOURCE_DIR}/../../..")
SET(ROCKSDB_PATH=${BUILD_PATH}/deps/rocksdb-5.3.6)
SET(LEVELDB_PATH=${BUILD_PATH}/deps/leveldb-1.18)
SET(JEMALLOC_PATH=${BUILD_PATH}/deps/jemalloc-4.1.0)
SET(SNAPPY_PATH=${BUILD_PATH}/deps/snappy-1.1.0)
SET(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build")

INCLUDE_DIRECTORIES(
    ${BUILD_PATH}/deps/rocksdb-5.3.6/include
    ${BUILD_PATH}/deps/rocksdb-5.3.6
    ${BUILD_PATH}/deps/jemalloc-4.1.0/include
    ${BUILD_PATH}/src
    ${BUILD_PATH}/src/client
    ${BUILD_PATH}/deps/bzip2-1.0.6
    ${BUILD_PATH}/deps/gflags-2.2.0/include
    ${BUILD_PATH}/deps/snappy-1.1.0
    ${BUILD_PATH}/tests/googletest/googlemock/include
    ${BUILD_PATH}/tests/googletest/googletest/include
    include
    )

LINK_DIRECTORIES(
    ${BUILD_PATH}/deps/rocksdb-5.3.6/
    ${BUILD_PATH}/deps/snappy-1.1.0/.libs
    ${BUILD_PATH}/deps/bzip2-1.0.6
    ${BUILD_PATH}/deps/jemalloc-4.1.0/lib/
    ${BUILD_PATH}/src/ssdb
    ${BUILD_PATH}/src/client
    ${BUILD_PATH}/src/util
    ${BUILD_PATH}/src/net
    ${BUILD_PATH}/src/codec
    ${BUILD_PATH}/deps/gflags-2.2.0/lib
    )
add_subdirectory(${BUILD_PATH}/tests/googletest gtest)
ADD_DEFINITIONS(-DGTESTING)
#AUX_SOURCE_DIRECTORY(. GTEST_SRC)
AUX_SOURCE_DIRECTORY(./codec GTEST_CODEC_SRC)

SET ( GTEST_SRC
    ${BUILD_PATH}/tests/googletest/googlemock/src/gmock_main.cc
)

IF(GCOV)
    ADD_DEFINITIONS("-fprofile-arcs -ftest-coverage")
    SET(CMAKE_EXE_LINKER_FLAGS "-fprofile-arcs -ftest-coverage")  
    SET(CMAKE_CXX_FLAGS "-DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O0 -Wno-sign-compare -std=c++11")
ELSE()
    SET(CMAKE_CXX_FLAGS "-DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare -std=c++11")
endif()

SET( SSDB_OBJS  
    ${BUILD_PATH}/src/ssdb/ssdb_impl.cpp
    ${BUILD_PATH}/src/ssdb/iterator.cpp
    ${BUILD_PATH}/src/ssdb/options.cpp
    ${BUILD_PATH}/src/ssdb/t_kv.cpp
    ${BUILD_PATH}/src/ssdb/t_keys.cpp
    ${BUILD_PATH}/src/ssdb/t_hash.cpp
    ${BUILD_PATH}/src/ssdb/t_zset.cpp
    ${BUILD_PATH}/src/ssdb/ttl.cpp
    ${BUILD_PATH}/src/ssdb/t_list.cpp
    ${BUILD_PATH}/src/ssdb/t_set.cpp
    ${BUILD_PATH}/src/ssdb/t_eset.cpp
    ${BUILD_PATH}/src/ssdb/t_cursor.cpp
)
SET( CODEC_OBJS
    ${BUILD_PATH}/src/codec/util.cpp
    ${BUILD_PATH}/src/codec/encode.cpp
    ${BUILD_PATH}/src/codec/decode.cpp
)

ADD_EXECUTABLE(ssdb-server 
    ${CODEC_OBJS}                                                                                                                                             
    ${GTEST_SRC}
    ${GTEST_CODEC_SRC}
    )

TARGET_LINK_LIBRARIES(ssdb-server gmock)

