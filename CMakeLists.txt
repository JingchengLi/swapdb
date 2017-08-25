cmake_minimum_required(VERSION 2.8)
project(swapdb)


add_subdirectory(./swap-redis-4.0.0 SWAP_REDIS)
add_subdirectory(./swap-ssdb-1.9.2 SWAP_SSDB)
