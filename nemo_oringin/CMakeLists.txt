cmake_minimum_required(VERSION 2.8)
project(nemo)
set (CMAKE_CXX_FLAGS "-Wall -W -Wno-unused-parameter -g -O2 -D__STDC_FORMAT_MACROS -fPIC -std=c++11")

INCLUDE_DIRECTORIES(
    ../rocksdb-4.9/include
    include
)

LINK_DIRECTORIES (
    3rdparty/rocksdb
)

SET ( SOURCES
    src/nemo_admin.cc
    src/nemo_backupable.cc
    src/nemo_bit.cc
    src/nemo.cc
    src/nemo_hash.cc
    src/nemo_iterator.cc
    src/nemo_kv.cc
    src/nemo_list.cc
    src/nemo_meta.cc
    src/nemo_set.cc
    src/nemo_zset.cc
    src/port.cc
    src/util.cc
    )

ADD_LIBRARY(libnemo-static STATIC ${SOURCES})
ADD_LIBRARY(libnemo-shared SHARED ${SOURCES})
set_target_properties(libnemo-static PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "nemo")

set_target_properties(libnemo-shared PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "nemo")

set_property(TARGET libnemo-static PROPERTY CXX_STANDARD 11)                                                                                                                              
set_property(TARGET libnemo-shared PROPERTY CXX_STANDARD 11)

TARGET_LINK_LIBRARIES ( libnemo-static librocksdb.so)
TARGET_LINK_LIBRARIES ( libnemo-shared librocksdb.so)

