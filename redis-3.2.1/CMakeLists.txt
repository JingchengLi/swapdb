cmake_minimum_required(VERSION 2.8)
project(redis)

cmake_policy(SET CMP0015 NEW)

SET(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/lib")
SET(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build")
set(CMAKE_C_FLAGS "-std=c99 -pedantic  -U__STRICT_ANSI__") # that would be bad

INCLUDE_DIRECTORIES(
        "deps/jemalloc/include"
        "deps/hiredis"
        "deps/linenoise"
        "deps/lua/src"
        "deps/geohash-int"
)

LINK_DIRECTORIES(
        "deps/jemalloc/lib"  #jemalloc
        "deps/hiredis"       #hiredis
        "deps/linenoise"
        "deps/lua/src"       #lua
        "deps/geohash-int"
)


ADD_LIBRARY(libgeohash-int deps/geohash-int)
SET_TARGET_PROPERTIES(libgeohash-int PROPERTIES LINKER_LANGUAGE C)

ADD_LIBRARY(libjemalloc deps/jemalloc/src)
SET_TARGET_PROPERTIES(libjemalloc PROPERTIES LINKER_LANGUAGE C)

ADD_LIBRARY(liblinenoise deps/linenoise)
SET_TARGET_PROPERTIES(liblinenoise PROPERTIES LINKER_LANGUAGE C)

SET ( SRC_HIREDIS
        deps/hiredis/async.c
        deps/hiredis/dict.c
        deps/hiredis/hiredis.c
        deps/hiredis/net.c
        deps/hiredis/sds.c
        deps/hiredis/test.c
        )
ADD_LIBRARY(libhiredis_static ${SRC_HIREDIS})
SET_TARGET_PROPERTIES(libhiredis_static PROPERTIES LINKER_LANGUAGE C)


SET ( SRC_LIBLUA
        deps/lua/src/fpconv.c
        deps/lua/src/lapi.c
        deps/lua/src/lauxlib.c
        deps/lua/src/lbaselib.c
        deps/lua/src/lcode.c
        deps/lua/src/ldblib.c
        deps/lua/src/ldebug.c
        deps/lua/src/ldo.c
        deps/lua/src/ldump.c
        deps/lua/src/lfunc.c
        deps/lua/src/lgc.c
        deps/lua/src/linit.c
        deps/lua/src/liolib.c
        deps/lua/src/llex.c
        deps/lua/src/lmathlib.c
        deps/lua/src/lmem.c
        deps/lua/src/loadlib.c
        deps/lua/src/lobject.c
        deps/lua/src/lopcodes.c
        deps/lua/src/loslib.c
        deps/lua/src/lparser.c
        deps/lua/src/lstate.c
        deps/lua/src/lstring.c
        deps/lua/src/lstrlib.c
        deps/lua/src/ltable.c
        deps/lua/src/ltablib.c
        deps/lua/src/ltm.c
        deps/lua/src/lundump.c
        deps/lua/src/lvm.c
        deps/lua/src/lzio.c
        deps/lua/src/lua_bit.c
        deps/lua/src/luac.c
        deps/lua/src/lua_cjson.c
        deps/lua/src/lua_cmsgpack.c
        deps/lua/src/lua_struct.c
        deps/lua/src/print.c
        deps/lua/src/strbuf.c )
ADD_LIBRARY(liblua_static ${SRC_LIBLUA})
TARGET_LINK_LIBRARIES ( liblua_static m dl)
SET_TARGET_PROPERTIES(liblua_static PROPERTIES LINKER_LANGUAGE C)


SET(redis-benchmark_files
        src/redis-benchmark.c
        src/ae.c
        src/anet.c
        src/sds.c
        src/adlist.c
        src/zmalloc.c
        )

ADD_EXECUTABLE(redis-benchmark ${redis-benchmark_files})
TARGET_LINK_LIBRARIES(redis-benchmark libhiredis_static jemalloc)

SET(redis-cli_files src/redis-cli.c
        src/anet.c
        src/sds.c
        src/adlist.c
        src/zmalloc.c
        src/release.c
        src/crc64.c
        src/ae.c
        deps/linenoise/linenoise.c
        )

ADD_EXECUTABLE(redis-cli ${redis-cli_files})
TARGET_LINK_LIBRARIES(redis-cli libhiredis_static m)


SET(redis-check-aof_files src/redis-check-aof.c)
ADD_EXECUTABLE(redis-check-aof ${redis-check-aof_files})

SET(redis-server_files src/adlist.c
        src/ae.c
        src/anet.c
        src/dict.c
        src/geo.c
        src/server.c
        src/sds.c
        src/zmalloc.c
        src/lzf_c.c
        src/lzf_d.c
        src/pqsort.c
        src/zipmap.c
        src/sha1.c
        src/ziplist.c
        src/release.c
        src/networking.c
        src/util.c
        src/quicklist.c
        src/object.c
        src/db.c
        src/replication.c
        src/redis-check-rdb.c
        src/rdb.c
        src/t_string.c
        src/t_list.c
        src/t_set.c
        src/t_zset.c
        src/t_hash.c
        src/config.c
        src/aof.c
        src/pubsub.c
        src/multi.c
        src/debug.c
        src/sort.c
        src/intset.c
        src/syncio.c
        src/cluster.c
        src/crc16.c
        src/endianconv.c
        src/slowlog.c
        src/scripting.c
        src/bio.c
        src/rio.c
        src/rand.c
        src/memtest.c
        src/crc64.c
        src/bitops.c
        src/sentinel.c
        src/notify.c
        src/setproctitle.c
        src/blocked.c
        src/hyperloglog.c
        src/latency.c
        src/sparkline.c
        deps/geohash-int/geohash.c
        deps/geohash-int/geohash_helper.c
        )
ADD_EXECUTABLE(redis-server ${redis-server_files})
TARGET_LINK_LIBRARIES(redis-server libgeohash-int libhiredis_static libjemalloc liblua_static m pthread)

#ADD_EXECUTABLE(redis-check-dump ${redis-server_files})
