#!/bin/sh

funStartServer(){
    cp ../../../../ssdb.conf ssdb_$1.config
    sed -ie "s/work_dir.*$/work_dir = \.\/var_$1/" ssdb_$1.config
    sed -ie "s/pidfile.*$/pidfile = \.\/var_$1\/ssdb\.pid/" ssdb_$1.config
    sed -ie "s/port:.*$/port: $1/" ssdb_$1.config
    sed -ie "s/output: .*$/output: log_$1.txt/" ssdb_$1.config
    mkdir -p var_$1
    # touch ./valgrind_$1.sup
    # valgrind --track-origins=yes --suppressions=./valgrind_$1.sup --show-reachable=no --show-possibly-lost=no --leak-check=full ../../../../ssdb-server -d ssdb_$1.config 2> ssdbstderr_$1
    ../../../../../build/ssdb-server -d ssdb_$1.config
}

killall -9 ssdb-server
rm -rf tmp/*
mkdir tmp
cd tmp
funStartServer 8888
funStartServer 8889
funStartServer 8890
