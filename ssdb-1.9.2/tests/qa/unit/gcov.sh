#!/bin/sh
# Collect coverage summary
CovInPath=CMakeFiles/test_codec.dir/root/WorkSpace/wy_redis/ssdb-1.9.2/src/codec
CovOutPath=/root/Windows/G-ToLinux/lcov-html
rm -f $CovInPath/*.gcda
cmake . -DGCOV=ON
make test_codec
./test_codec $*
geninfo -o ./unit.info $CovInPath
genhtml --legend --output-directory ./ --show-details -o $CovOutPath ./unit.info
