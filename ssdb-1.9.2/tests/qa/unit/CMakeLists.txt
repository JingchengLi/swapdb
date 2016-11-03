cmake_minimum_required(VERSION 2.8)
project(ssdb-test)

SET(BUILD_PATH "${CMAKE_SOURCE_DIR}/../../..")
SET(LEVELDB_PATH=${BUILD_PATH}/deps/leveldb-1.18)
SET(JEMALLOC_PATH=${BUILD_PATH}/deps/jemalloc-4.1.0)
SET(SNAPPY_PATH=${BUILD_PATH}/deps/snappy-1.1.0)

INCLUDE_DIRECTORIES(
    ${BUILD_PATH}/deps/leveldb-1.18/include
    ${BUILD_PATH}/deps/leveldb-1.18/include/leveldb
    ${BUILD_PATH}/deps/jemalloc-4.1.0/include
    ${BUILD_PATH}/src
    ${BUILD_PATH}/src/client
    ${BUILD_PATH}/tests/gtest_1.7.0/include
    ${BUILD_PATH}/src/codec/
    )

LINK_DIRECTORIES(
    ${BUILD_PATH}/deps/leveldb-1.18/
    ${BUILD_PATH}/deps/snappy-1.1.0/.libs
    ${BUILD_PATH}/deps/jemalloc-4.1.0/lib/
    ${BUILD_PATH}/src/ssdb
    ${BUILD_PATH}/src/client
    ${BUILD_PATH}/src/util
    ${BUILD_PATH}/src/net
    ${BUILD_PATH}/src/codec
    )
add_subdirectory(${BUILD_PATH}/tests/gtest_1.7.0 gtest)
ADD_DEFINITIONS(-DGTESTING)
AUX_SOURCE_DIRECTORY(. GTEST_SRC)
AUX_SOURCE_DIRECTORY(./codec GTEST_CODEC_SRC)

IF(GCOV)
    ADD_DEFINITIONS("-fprofile-arcs -ftest-coverage")
    SET(CMAKE_EXE_LINKER_FLAGS "-fprofile-arcs -ftest-coverage")  
endif()

SET(CMAKE_CXX_FLAGS "-DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare")

ADD_LIBRARY(libjemalloc-static ${BUILD_PATH}/deps/jemalloc-4.1.0/lib)
SET_TARGET_PROPERTIES(libjemalloc-static PROPERTIES 
    LINKER_LANGUAGE C
    OUTPUT_NAME "jemalloc")

ADD_LIBRARY(libsnappy-static ${BUILD_PATH}/deps/snappy-1.1.0/.libs)
SET_TARGET_PROPERTIES(libsnappy-static PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "snappy")

ADD_LIBRARY(libleveldb-static ${BUILD_PATH}/deps/leveldb-1.18)
SET_TARGET_PROPERTIES(libleveldb-static PROPERTIES 
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "leveldb")

# src/ssdb
# libssdb.a
SET( SSDB_OBJS  
    ${BUILD_PATH}/src/ssdb/ssdb_impl.cpp
    ${BUILD_PATH}/src/ssdb/iterator.cpp
    ${BUILD_PATH}/src/ssdb/options.cpp                                                                                                                                                      
    ${BUILD_PATH}/src/ssdb/t_kv.cpp
    ${BUILD_PATH}/src/ssdb/t_hash.cpp
    ${BUILD_PATH}/src/ssdb/t_zset.cpp
    ${BUILD_PATH}/src/ssdb/t_queue.cpp
    ${BUILD_PATH}/src/ssdb/binlog.cpp
    ${BUILD_PATH}/src/ssdb/ttl.cpp
)

ADD_LIBRARY(libssdb-static STATIC ${SSDB_OBJS})                                                                                                                                                  
#ADD_LIBRARY(libssdb-shared SHARED ${SSDB_OBJS})
TARGET_LINK_LIBRARIES ( libssdb-static leveldb snappy )
#TARGET_LINK_LIBRARIES ( libssdb-shared leveldb )
set_target_properties(libssdb-static PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "ssdb")
#set_target_properties(libssdb-shared PROPERTIES
#    LINKER_LANGUAGE CXX
#    OUTPUT_NAME "ssdb")

# src/client
# libssdb-client.a

SET( CLIENT_OBJS
    ${BUILD_PATH}/src/client/SSDB_impl.cpp
    ${BUILD_PATH}/src/client/../util/bytes.cpp
    ${BUILD_PATH}/src/client/../net/link.cpp
)

ADD_LIBRARY(libssdb-client-static STATIC ${CLIENT_OBJS})                                                                                                                                                  
ADD_LIBRARY(libssdb-client-shared SHARED ${CLIENT_OBJS})
set_target_properties(libssdb-client-static PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "ssdb-client")
set_target_properties(libssdb-client-shared PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "ssdb-client")

# src/codec
# libcodec.a
SET( CODEC_OBJS
    ${BUILD_PATH}/src/codec/util.cpp
    ${BUILD_PATH}/src/codec/encode.cpp
)

ADD_LIBRARY(libcodec-static STATIC ${CODEC_OBJS})
ADD_LIBRARY(libcodec-shared SHARED ${CODEC_OBJS})
set_target_properties(libcodec-static PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "codec")
set_target_properties(libcodec-shared PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "codec")

# src/net
# libnet.a

SET( NET_OBJS 
    ${BUILD_PATH}/src/net/server.cpp
    ${BUILD_PATH}/src/net/resp.cpp
    ${BUILD_PATH}/src/net/proc.cpp
    ${BUILD_PATH}/src/net/worker.cpp
    ${BUILD_PATH}/src/net/fde.cpp
    ${BUILD_PATH}/src/net/link.cpp
)

ADD_LIBRARY(libnet-static STATIC ${NET_OBJS})                                                                                                                                                  
ADD_LIBRARY(libnet-shared SHARED ${NET_OBJS})
set_target_properties(libnet-static PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "net")
set_target_properties(libnet-shared PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "net")


# src/util
# libutil.a

SET( UTIL_OBJS 
    ${BUILD_PATH}/src/util/log.cpp
    ${BUILD_PATH}/src/util/config.cpp
    ${BUILD_PATH}/src/util/bytes.cpp
    ${BUILD_PATH}/src/util/sorted_set.cpp
    ${BUILD_PATH}/src/util/app.cpp
)

ADD_LIBRARY(libutil-static STATIC ${UTIL_OBJS})                                                                                                                                                  
ADD_LIBRARY(libutil-shared SHARED ${UTIL_OBJS})
set_target_properties(libutil-static PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "util")
set_target_properties(libutil-shared PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "util")

# src

SET( SRC_OBJS
    ${GTEST_SRC}
    ${GTEST_CODEC_SRC}
    ${BUILD_PATH}/src/ssdb-server.cpp
    ${BUILD_PATH}/src/client/SSDB_impl.cpp
    ${BUILD_PATH}/src/proc_kv.cpp
    ${BUILD_PATH}/src/proc_hash.cpp
    ${BUILD_PATH}/src/proc_zset.cpp
    ${BUILD_PATH}/src/proc_queue.cpp
    ${BUILD_PATH}/src/backend_dump.cpp
    ${BUILD_PATH}/src/backend_sync.cpp
    ${BUILD_PATH}/src/slave.cpp
    ${BUILD_PATH}/src/serv.cpp
    ${BUILD_PATH}/src/proc_cluster.cpp
    ${BUILD_PATH}/src/cluster.cpp
    ${BUILD_PATH}/src/cluster_store.cpp
    ${BUILD_PATH}/src/cluster_migrate.cpp
)

ADD_EXECUTABLE(ssdb-server ${SRC_OBJS})                                                                                                                                             
TARGET_LINK_LIBRARIES(ssdb-server libssdb-static libutil-static libnet-static leveldb snappy jemalloc pthread rt gtest libcodec-static) 
