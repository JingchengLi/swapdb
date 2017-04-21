cmake_minimum_required(VERSION 2.8)
project(ssdb)

option(ROCKSDB "enable rocksdb support" ON)
option(PTIMER "performance monitor" ON)

SET(LEVELDB_PATH=${CMAKE_CURRENT_SOURCE_DIR}/deps/leveldb-1.18)
SET(JEMALLOC_PATH=${CMAKE_CURRENT_SOURCE_DIR}/deps/jemalloc-4.1.0)
SET(SNAPPY_PATH=${CMAKE_CURRENT_SOURCE_DIR}/deps/snappy-1.1.0)
SET(CMAKE_C_FLAGS "--std=gnu99 ${CMAKE_C_FLAGS}")

execute_process(COMMAND                                                                                                                                                                        
    "./build.sh"
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)

INCLUDE_DIRECTORIES(
    ${CMAKE_CURRENT_SOURCE_DIR}/deps/jemalloc-4.1.0/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/client
    )


LINK_DIRECTORIES(
    ${CMAKE_CURRENT_SOURCE_DIR}/deps/snappy-1.1.0/.libs
    ${CMAKE_CURRENT_SOURCE_DIR}/deps/jemalloc-4.1.0/lib/
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ssdb
    ${CMAKE_CURRENT_SOURCE_DIR}/src/client
    ${CMAKE_CURRENT_SOURCE_DIR}/src/util
    ${CMAKE_CURRENT_SOURCE_DIR}/src/net
        /usr/local/lib
    )


IF(GCOV)
    ADD_DEFINITIONS("-fprofile-arcs -ftest-coverage")
    SET(CMAKE_EXE_LINKER_FLAGS "-fprofile-arcs -ftest-coverage")  
    SET(CMAKE_CXX_FLAGS "-DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O0 -Wno-sign-compare -std=c++11")
ELSE()
    SET(CMAKE_CXX_FLAGS "-DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O0 -Wno-sign-compare -std=c++11")
ENDIF()



IF(ROCKSDB)
    message("==================use rocksdb===================")
    INCLUDE_DIRECTORIES(
            ${CMAKE_CURRENT_SOURCE_DIR}/deps/rocksdb-4.11.2/include
    )
    LINK_DIRECTORIES(
            ${CMAKE_CURRENT_SOURCE_DIR}/deps/rocksdb-4.11.2/third-party/gtest-1.7.0/fused-src/gtest
            ${CMAKE_CURRENT_SOURCE_DIR}/deps/rocksdb-4.11.2/
    )
    SET(CMAKE_CXX_FLAGS "-DNDEBUG -D__STDC_FORMAT_MACROS -Wall -g -O0 -Wno-sign-compare -std=c++11")
    ADD_LIBRARY(librocksdb-static deps/rocksdb-4.11.2)
    TARGET_LINK_LIBRARIES ( librocksdb-static snappy gtest gflags )
    SET_TARGET_PROPERTIES(librocksdb-static PROPERTIES
            LINKER_LANGUAGE CXX
            OUTPUT_NAME "rocksdb")
ELSE()
    message("==================use leveldb===================")
    INCLUDE_DIRECTORIES(
            ${CMAKE_CURRENT_SOURCE_DIR}/deps/leveldb-1.18/include
            ${CMAKE_CURRENT_SOURCE_DIR}/deps/leveldb-1.18/include/leveldb
    )
    LINK_DIRECTORIES(
            ${CMAKE_CURRENT_SOURCE_DIR}/deps/leveldb-1.18/
    )
    SET(CMAKE_CXX_FLAGS "-DNDEBUG -DUSE_LEVELDB -D__STDC_FORMAT_MACROS -g -Wall -O0 -Wno-sign-compare -std=c++11")
    ADD_LIBRARY(libleveldb-static deps/leveldb-1.18)
    SET_TARGET_PROPERTIES(libleveldb-static PROPERTIES
            LINKER_LANGUAGE CXX
            OUTPUT_NAME "leveldb")
ENDIF()


IF(PTIMER)
    add_compile_options(-DPTIMER)
ENDIF()


IF(DREPLY)
    add_compile_options(-DDREPLY)
ENDIF()


ADD_LIBRARY(libjemalloc-static deps/jemalloc-4.1.0/lib)
SET_TARGET_PROPERTIES(libjemalloc-static PROPERTIES 
    LINKER_LANGUAGE C
    OUTPUT_NAME "jemalloc")

ADD_LIBRARY(libsnappy-static deps/snappy-1.1.0/.libs)
SET_TARGET_PROPERTIES(libsnappy-static PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "snappy")


SET( SSDB_OBJS  
src/ssdb/ssdb_impl.cpp
src/ssdb/iterator.cpp
src/ssdb/options.cpp                                                                                                                                                      
src/ssdb/t_kv.cpp
src/ssdb/t_hash.cpp
src/ssdb/t_zset.cpp
src/ssdb/ttl.cpp
src/ssdb/t_list.cpp
src/ssdb/t_set.cpp
src/ssdb/t_eset.cpp
src/ssdb/internal_error.cpp
src/ssdb/t_cursor.cpp
        )


ADD_LIBRARY(libssdb-static STATIC ${SSDB_OBJS})                                                                                                                                                  
#ADD_LIBRARY(libssdb-shared SHARED ${SSDB_OBJS})


IF(ROCKSDB)
    TARGET_LINK_LIBRARIES ( libssdb-static rocksdb snappy z bz2)
    #TARGET_LINK_LIBRARIES ( libssdb-shared rocksdb )
ELSE()
    TARGET_LINK_LIBRARIES ( libssdb-static leveldb snappy )
    #TARGET_LINK_LIBRARIES ( libssdb-shared leveldb )
ENDIF()

set_target_properties(libssdb-static PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "ssdb")
#set_target_properties(libssdb-shared PROPERTIES
#    LINKER_LANGUAGE CXX
#    OUTPUT_NAME "ssdb")

# src/client
# libssdb-client.a

SET( CLIENT_OBJS
src/client/hello-ssdb.cpp
src/client/SSDB_impl.cpp
src/client/../util/bytes.cpp
src/client/../net/link.cpp
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
src/codec/util.cpp
src/codec/encode.cpp
src/codec/decode.cpp
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
src/util/log.cpp
src/util/config.cpp
src/util/bytes.cpp
#src/util/sorted_set.cpp
src/util/app.cpp
src/util/backtrace.cpp
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

SET( RDB_OBJS
        src/redis/crc/crc64speed.c
        src/redis/crc/crcspeed.c
src/redis/ziplist.c
src/redis/intset.c
src/redis/lzf_c.c
src/redis/lzf_d.c
src/redis/endianconv.c
src/redis/zmalloc.c
src/redis/rdb_encoder.cpp
src/redis/rdb_decoder.cpp
)


ADD_LIBRARY(librdb-static STATIC ${RDB_OBJS})
ADD_LIBRARY(librdb-shared SHARED ${RDB_OBJS})
set_target_properties(librdb-static PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "rdb")
set_target_properties(librdb-shared PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "rdb")

SET( SRC_OBJS
src/ssdb-server.cpp
src/client/SSDB_impl.cpp
src/bproc_data.cpp
src/proc_kv.cpp
src/proc_hash.cpp
src/proc_set.cpp
src/proc_zset.cpp
src/proc_queue.cpp
src/backend_dump.cpp
src/serv.cpp
)

ADD_EXECUTABLE(ssdb-server ${SRC_OBJS})
#TARGET_LINK_LIBRARIES(ssdb-server libssdb-static libutil-static libnet-static libcodec-static leveldb gtest gflags snappy jemalloc pthread rt z bz2)


IF(ROCKSDB)
    TARGET_LINK_LIBRARIES(ssdb-server libssdb-static librdb-static libutil-static libnet-static libcodec-static rocksdb gtest gflags snappy jemalloc pthread rt z bz2)
ELSE()
    TARGET_LINK_LIBRARIES(ssdb-server leveldb libssdb-static librdb-static libutil-static libnet-static libcodec-static gflags snappy jemalloc pthread rt z bz2)
ENDIF()

#ADD_EXECUTABLE(hello-ssdb ${CLIENT_OBJS})


ADD_EXECUTABLE(test_link src/net/test_link.cpp)
TARGET_LINK_LIBRARIES(test_link libnet-static libutil-static libcodec-static)


IF(QA)
add_subdirectory(tests/qa/fake)
ENDIF()
