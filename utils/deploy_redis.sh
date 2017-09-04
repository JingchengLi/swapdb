#!/bin/sh

SSDB_CFG=ssdb.conf.template
REDIS_CFG=redis.conf.template
SSDB=../../../build/ssdb-server
REDIS=../../../build/redis-server

if [ -n "$1" ];then
    REDISPORT=$1
else
    REDISPORT=6379
fi
DIR=./tmp/${REDISPORT}_dir

cleanup(){
    killall -9 ssdb-server
    killall -9 redis-server
    rm -r $DIR
}

prepare_dir() {
    mkdir -p $DIR
    cp $SSDB_CFG $REDIS_CFG $DIR
}

startSSDBServer(){
    sed -i "s/6379/$REDISPORT/g"  $SSDB_CFG
    $SSDB -d $SSDB_CFG
}

startRedisServer(){
    sed -i "s/port [0-9]\{1,\}/port $REDISPORT/g"  $REDIS_CFG
    $REDIS $REDIS_CFG &> /dev/null &
}

startServer(){
    cd $DIR
    startSSDBServer
    sleep 1
    startRedisServer
    sleep 1
}

cleanup
prepare_dir
startServer
