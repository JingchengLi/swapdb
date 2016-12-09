#!/bin/sh

PROJECT_DIR=`pwd`

cd deps
#make lua
make jemalloc

cd ${PROJECT_DIR}/src
./mkreleasehdr.sh
