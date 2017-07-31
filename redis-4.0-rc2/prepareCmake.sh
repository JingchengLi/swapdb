#!/bin/sh

PROJECT_DIR=`pwd`

cd deps
make lua hiredis jemalloc

cd ${PROJECT_DIR}/src
./mkreleasehdr.sh
