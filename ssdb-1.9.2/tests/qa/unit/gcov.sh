#!/bin/sh
# Collect coverage summary
CovInPath=CMakeFiles/ssdb-server.dir/home/zl/workspace/wy_redis/ssdb-1.9.2/src/codec
CovOutPath=~/report/lcov-html
# CovOutPath=/root/Windows/G-ToLinux/lcov-html
rm -f $CovInPath/*.gcda
cmake . -DGCOV=ON
make ssdb-server
./build/ssdb-server $*
geninfo -o ./unit.info $CovInPath
genhtml --legend --output-directory ./ --show-details -o $CovOutPath ./unit.info
