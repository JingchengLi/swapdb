cmake_minimum_required(VERSION 2.6.4)
SET(CMAKE_SUPPRESS_REGENERATION 1)
SET(CMAKE_USE_RELATIVE_PATHS ON)

PROJECT(redis)

set(VERSION "3.0.4")
set(CPACK_PACKAGE_VERSION "1.2")
set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_NAME "redis_${VERSION}")

set(CPACK_PACKAGE_CONTACT "fushaoping@chinabank.com.cn")
set(CPACK_PACKAGE_VENDOR "chinabank")
set(CPACK_PACKAGING_INSTALL_PREFIX "/opt/wy")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}.${CMAKE_SYSTEM_PROCESSOR}")

SET(PROJECT_ROOT_PATH 		"${CMAKE_SOURCE_DIR}/../")
SET(LIBRARY_OUTPUT_PATH		"${PROJECT_ROOT_PATH}/lib")
SET(EXECUTABLE_OUTPUT_PATH 	"${PROJECT_ROOT_PATH}/bin")

ADD_DEFINITIONS(-g -W -Wall -D_REENTRANT -D_FILE_OFFSET_BITS=64)

INCLUDE_DIRECTORIES(		"${CMAKE_SOURCE_DIR}"
							"${PROJECT_ROOT_PATH}/deps/jemalloc/include"
							"${PROJECT_ROOT_PATH}/deps/hiredis"
							"${PROJECT_ROOT_PATH}/deps/linenoise"
							"${PROJECT_ROOT_PATH}/deps/zookeeper/include"
							"${PROJECT_ROOT_PATH}/deps/zookeeper/generated"
							"${PROJECT_ROOT_PATH}/deps/lua/src")

LINK_DIRECTORIES(			"${PROJECT_ROOT_PATH}/lib"
							"${PROJECT_ROOT_PATH}/../../libs"
							"${CPACK_PACKAGING_INSTALL_PREFIX}/redis_${VERSION}"
							"${CPACK_PACKAGING_INSTALL_PREFIX}/redis_${VERSION}/libs"
				)

#ADD_SUBDIRECTORY(deps)                          
#ADD_SUBDIRECTORY(redis_bin)
#EXECUTE_PROCESS(COMMAND ${PROJECT_ROOT_PATH}/make)

install (PROGRAMS ../src/redis-server       ../src/redis-cli 
        ../src/redis-benchmark              ../src/redis-check-dump 
        ../src/redis-check-aof              ./cluster_config.sh  
        ./redis_xxxx.conf.template		    ./redis_xxxx_init_script.template
        ./crontab_xxxx.template             DESTINATION "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}")

include(CPack)
