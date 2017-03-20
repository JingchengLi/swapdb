#!/bin/bash
if [ x"" != x"`pidof ssdb-server`" ];then
    echo "ssdb server is already running!"
    exit
fi
cd `pwd`
rm var/ssdb.pid
./ssdb-server -d ssdb.conf -s start
