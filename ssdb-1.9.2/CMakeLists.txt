cmake_minimum_required(VERSION 2.8.12)
project(ssdb)

option(PTIMER "performance monitor" ON)

SET(ROCKSDB_PATH ${CMAKE_CURRENT_SOURCE_DIR}/deps/rocksdb-5.3.6)
SET(JEMALLOC_PATH ${CMAKE_CURRENT_SOURCE_DIR}/deps/jemalloc-4.1.0)
SET(SNAPPY_PATH ${CMAKE_CURRENT_SOURCE_DIR}/deps/snappy-1.1.0)
SET(GFLAGS_PATH ${CMAKE_CURRENT_SOURCE_DIR}/deps/gflags-2.2.0)
SET(BZIP2_PATH ${CMAKE_CURRENT_SOURCE_DIR}/deps/bzip2-1.0.6)

# Set a default build type if none was specified
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Release" "Debug" "MinSizeRel" "RelWithDebInfo")
endif ()

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

execute_process(COMMAND
        "./build.sh"
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        )

INCLUDE_DIRECTORIES(
        ${GFLAGS_PATH}/include
        ${SNAPPY_PATH}
        ${JEMALLOC_PATH}/include
        ${BZIP2_PATH}
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/src/client

)


LINK_DIRECTORIES(
        ${BZIP2_PATH}
        ${GFLAGS_PATH}/lib
        ${SNAPPY_PATH}/.libs
        ${JEMALLOC_PATH}/lib/
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ssdb
        ${CMAKE_CURRENT_SOURCE_DIR}/src/client
        ${CMAKE_CURRENT_SOURCE_DIR}/src/util
        ${CMAKE_CURRENT_SOURCE_DIR}/src/net
)

# BUILD FLAGS

SET(CMAKE_C_FLAGS "--std=gnu99 ${CMAKE_C_FLAGS}")

IF (GCOV)
    ADD_DEFINITIONS("-fprofile-arcs -ftest-coverage")
    SET(CMAKE_EXE_LINKER_FLAGS "-fprofile-arcs -ftest-coverage")
    SET(CMAKE_CXX_FLAGS " -D__STDC_FORMAT_MACROS -Wall -g -O0 -Wno-sign-compare -std=c++11")
ELSE ()
    SET(CMAKE_CXX_FLAGS " -D__STDC_FORMAT_MACROS -Wall -g -Wno-sign-compare -std=c++11") #-DNDEBUG
ENDIF ()

IF (PTIMER)
    add_definitions(-DPTIMER)
ENDIF ()


IF (DREPLY)
    add_definitions(-DDREPLY)
ENDIF ()


INCLUDE_DIRECTORIES(
        ${ROCKSDB_PATH}/include
        ${ROCKSDB_PATH}
)
LINK_DIRECTORIES(
        ${ROCKSDB_PATH}/third-party/gtest-1.7.0/fused-src/gtest
        ${ROCKSDB_PATH}/
)
ADD_LIBRARY(librocksdb-static ${ROCKSDB_PATH})
TARGET_LINK_LIBRARIES(librocksdb-static snappy.a gtest gflags)
SET_TARGET_PROPERTIES(librocksdb-static PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "rocksdb")


SET(SSDB_OBJS
        src/ssdb/ssdb_impl.cpp
        src/ssdb/iterator.cpp
        src/ssdb/options.cpp
        src/ssdb/t_kv.cpp
        src/ssdb/t_keys.cpp
        src/ssdb/t_hash.cpp
        src/ssdb/t_zset.cpp
        src/ssdb/ttl.cpp
        src/ssdb/t_list.cpp
        src/ssdb/t_set.cpp
        src/ssdb/t_eset.cpp
        src/ssdb/t_cursor.cpp
        )


ADD_LIBRARY(libssdb-static STATIC ${SSDB_OBJS})
TARGET_LINK_LIBRARIES(libssdb-static rocksdb snappy.a z bz2)
set_target_properties(libssdb-static PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "ssdb")


SET(CLIENT_OBJS
        src/client/hello-ssdb.cpp
        src/client/SSDB_impl.cpp
        src/client/../util/bytes.cpp
        src/client/../net/link.cpp
        )

ADD_LIBRARY(libssdb-client-static STATIC ${CLIENT_OBJS})
set_target_properties(libssdb-client-static PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "ssdb-client")


SET(CODEC_OBJS
        src/codec/util.cpp
        src/codec/encode.cpp
        src/codec/decode.cpp
        )

ADD_LIBRARY(libcodec-static STATIC ${CODEC_OBJS})
set_target_properties(libcodec-static PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "codec")


SET(NET_OBJS
        src/net/server.cpp
        src/net/resp.cpp
        src/net/proc.cpp
        src/net/worker.cpp
        src/net/fde.cpp
        src/net/link.cpp
        src/net/redis/redis_client.cpp
        src/net/redis/redis_stream.cpp
        src/net/redis/transfer.cpp
        )

ADD_LIBRARY(libnet-static STATIC ${NET_OBJS})
set_target_properties(libnet-static PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "net")


SET(UTIL_OBJS
        src/util/file2.cpp
        src/util/log.cpp
        src/util/config.cpp
        src/util/bytes.cpp
        #src/util/sorted_set.cpp
        src/util/app.cpp
        src/util/backtrace.cpp
        src/util/internal_error.cpp
        )

ADD_LIBRARY(libutil-static STATIC ${UTIL_OBJS})
set_target_properties(libutil-static PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "util")


SET(RDB_OBJS
        src/redis/crc/crc64speed.c
        src/redis/crc/crcspeed.c
        src/redis/ziplist.c
        src/redis/intset.c
        src/redis/lzf_c.c
        src/redis/lzf_d.c
        src/redis/endianconv.c
        src/redis/zmalloc.c
        src/redis/redis_encoder.cpp
        src/redis/rdb_decoder.cpp
        src/redis/sha1.c
        )


ADD_LIBRARY(librdb-static STATIC ${RDB_OBJS})
set_target_properties(librdb-static PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "rdb")


SET(SRC_OBJS
        src/ssdb-server.cpp
        src/client/SSDB_impl.cpp
        src/bproc_data.cpp
        src/proc_kv.cpp
        src/proc_hash.cpp
        src/proc_set.cpp
        src/proc_zset.cpp
        src/proc_queue.cpp
        src/serv.cpp
        src/replication.cpp
        src/replication2.cpp
        src/replication_slave2.cpp
        )

ADD_EXECUTABLE(ssdb-server ${SRC_OBJS})


TARGET_LINK_LIBRARIES(ssdb-server libssdb-static librdb-static libutil-static libnet-static libcodec-static rocksdb gflags snappy.a jemalloc.a pthread) # z bz2
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    #nothing
ELSE ()
    TARGET_LINK_LIBRARIES(ssdb-server rt) # z bz2
endif ()


IF (QA)
    add_subdirectory(tests/qa/fake)
ENDIF ()

