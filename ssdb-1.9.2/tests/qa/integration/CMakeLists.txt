cmake_minimum_required(VERSION 2.8)
project(integ-test)

SET(BUILD_PATH "${CMAKE_SOURCE_DIR}/../../..")
INCLUDE_DIRECTORIES(
    ${BUILD_PATH}/tests/gtest_1.7.0/include
    ${BUILD_PATH}/src
    ${BUILD_PATH}/src/client
    include
    )

add_subdirectory(${BUILD_PATH}/tests/gtest_1.7.0 gtest)
ADD_DEFINITIONS(-DGTESTING)
AUX_SOURCE_DIRECTORY(. GTEST_SRC)
#AUX_SOURCE_DIRECTORY(./codec GTEST_CODEC_SRC)

IF(GCOV)
    ADD_DEFINITIONS("-fprofile-arcs -ftest-coverage")
    SET(CMAKE_EXE_LINKER_FLAGS "-fprofile-arcs -ftest-coverage")  
    SET(CMAKE_CXX_FLAGS "-DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O0 -Wno-sign-compare -std=c++11")
ELSE()
    SET(CMAKE_CXX_FLAGS "-DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare -std=c++11")
ENDIF()

IF(ROBUST)
    ADD_DEFINITIONS(-DROBUST)
ENDIF()

# src/client
# libssdb-client.a

SET( CLIENT_OBJS
    ${BUILD_PATH}/src/client/SSDB_impl.cpp
    ${BUILD_PATH}/src/client/../util/bytes.cpp
    ${BUILD_PATH}/src/client/../net/link.cpp
)

ADD_LIBRARY(libssdb-client-static STATIC ${CLIENT_OBJS})                                                                                                                                                  
set_target_properties(libssdb-client-static PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "ssdb-client")

# src/util
# libutil.a

SET( UTIL_OBJS
    ${BUILD_PATH}/src/util/log.cpp
    ${BUILD_PATH}/src/util/config.cpp
    ${BUILD_PATH}/src/util/bytes.cpp
    #${BUILD_PATH}/src/util/sorted_set.cpp
    ${BUILD_PATH}/src/util/app.cpp
    ${BUILD_PATH}/src/util/thread.cpp
)

ADD_LIBRARY(libutil-static STATIC ${UTIL_OBJS})
ADD_LIBRARY(libutil-shared SHARED ${UTIL_OBJS})
set_target_properties(libutil-static PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "util")
set_target_properties(libutil-shared PROPERTIES
    LINKER_LANGUAGE CXX
    OUTPUT_NAME "util")

ADD_EXECUTABLE(integ-test
    ${GTEST_SRC})

TARGET_LINK_LIBRARIES(integ-test gtest libssdb-client-static libutil-static)
