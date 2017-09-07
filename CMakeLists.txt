cmake_minimum_required(VERSION 2.8)
project(swapdb)


add_subdirectory(./swap-redis-4.0.0 ${PROJECT_SOURCE_DIR}/build/SWAP_REDIS)
add_subdirectory(./swap-ssdb-1.9.2 ${PROJECT_SOURCE_DIR}/build/SWAP_SSDB)
