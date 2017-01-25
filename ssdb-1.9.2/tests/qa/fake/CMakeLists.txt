cmake_minimum_required(VERSION 2.8)
project(integ-test)

SET(BUILD_PATH "${CMAKE_SOURCE_DIR}")
INCLUDE_DIRECTORIES(
    ${BUILD_PATH}/tests/gtest_1.7.0/include
    ${BUILD_PATH}/src
    ${BUILD_PATH}/src/client
    include
    ../integration/include
    )

add_subdirectory(${BUILD_PATH}/tests/gtest_1.7.0 gtest)
ADD_DEFINITIONS(-DGTESTING)
AUX_SOURCE_DIRECTORY(../integration/ GTEST_SRC)
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


ADD_EXECUTABLE(integ-test
    ${GTEST_SRC})

TARGET_LINK_LIBRARIES(integ-test gtest libssdb-client-static libutil-static)
