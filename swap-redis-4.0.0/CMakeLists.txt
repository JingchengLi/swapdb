cmake_minimum_required(VERSION 2.8)
project(redis)

cmake_policy(SET CMP0015 NEW)

execute_process(COMMAND
        "./prepareCmake.sh"
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)

add_definitions(-Wall)
SET(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/lib")
SET(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build")
IF(GCOV)
    ADD_DEFINITIONS("-fprofile-arcs -ftest-coverage")
    SET(CMAKE_EXE_LINKER_FLAGS "-fprofile-arcs -ftest-coverage")
    set(CMAKE_C_FLAGS "-g -std=c99 -pedantic -rdynamic -DUSE_JEMALLOC -U__STRICT_ANSI__ -DMULTIPLE_DC -DUSE_CLUSTER_PROTOCOL_V3") # that would be bad
ELSE()
    set(CMAKE_C_FLAGS "-g -std=c99 -pedantic -rdynamic -DUSE_JEMALLOC -U__STRICT_ANSI__ -DMULTIPLE_DC -DUSE_CLUSTER_PROTOCOL_V3") # that would be bad
ENDIF()

INCLUDE_DIRECTORIES(
        "deps/jemalloc/include"
        "deps/hiredis"
        "deps/linenoise"
        "deps/lua/src"
        "src"
)

LINK_DIRECTORIES(
        "deps/jemalloc/lib"  #jemalloc
        "deps/hiredis"       #hiredis
        "deps/linenoise"
        "deps/lua/src"       #lua
)

SET(redis-benchmark_files
        src/redis-benchmark.c
        src/ae.c
        src/anet.c
        src/adlist.c
        src/zmalloc.c
        )

ADD_EXECUTABLE(redis-benchmark ${redis-benchmark_files})
TARGET_LINK_LIBRARIES(redis-benchmark hiredis jemalloc pthread)

SET(redis-cli_files
        src/redis-cli.c
        src/anet.c
        src/adlist.c
        src/zmalloc.c
        src/release.c
        src/crc64.c
        src/ae.c
        deps/linenoise/linenoise.c
        )

ADD_EXECUTABLE(redis-cli ${redis-cli_files})
TARGET_LINK_LIBRARIES(redis-cli hiredis m jemalloc pthread)

SET(redis-server_files
        src/adlist.c
        src/ae.c
        src/anet.c
        src/aof.c
        src/bio.c
        src/bitops.c
        src/blocked.c
        src/childinfo.c
        #src/cluster.c
        src/cluster_v3.c
        src/config.c
        src/crc16.c
        src/crc64.c
        src/db.c
        src/dict.c
        src/debug.c
        src/endianconv.c
        src/evict.c
        src/expire.c
        src/geo.c
        src/geohash.c
        src/geohash_helper.c
        src/hyperloglog.c
        src/intset.c
        src/latency.c
        src/lazyfree.c
        src/lzf_c.c
        src/lzf_d.c
        src/memtest.c
        src/module.c
        src/multi.c
        src/networking.c
        src/notify.c
        src/object.c
        src/pqsort.c
        src/pubsub.c
        src/quicklist.c
        src/rand.c
        src/rdb.c
        src/redis-check-rdb.c
        src/redis-check-aof.c
        src/release.c
        src/replication.c
        src/rio.c
        src/scripting.c
        src/sds.c
        src/server.c
        src/rax.c
        src/siphash.c
        src/defrag.c
        src/sentinel.c
        src/setproctitle.c
        src/sha1.c
        src/slowlog.c
        src/sort.c
        src/sparkline.c
        src/syncio.c
        src/t_hash.c
        src/t_list.c
        src/t_set.c
        src/t_string.c
        src/t_zset.c
        src/util.c
        src/ziplist.c
        src/zipmap.c
        src/zmalloc.c
        )
ADD_EXECUTABLE(redis-server ${redis-server_files})
TARGET_LINK_LIBRARIES(redis-server hiredis jemalloc lua m pthread dl)


ADD_EXECUTABLE(redis-check-rdb ${redis-server_files})
TARGET_LINK_LIBRARIES(redis-check-rdb hiredis jemalloc lua m pthread dl)

