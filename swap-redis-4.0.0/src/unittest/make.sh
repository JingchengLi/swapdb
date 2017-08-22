rm ./core 2>/dev/null
gcc -g testhotpool.c sds.c -o testhotpool
gcc -g test_mem_crash.c -o testmemcrash
