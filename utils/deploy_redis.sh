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

prepare_dir() {
    mkdir -p $DIR
    cp $SSDB_CFG $REDIS_CFG $DIR
}

startSSDBServer(){
    if [[ ! -f "$SSDB" ]]; then
        echo "$SSDB not exists, must build it at first!"
        exit 0
    fi
    sed -i "s/6379/$REDISPORT/g" $SSDB_CFG
    mv $SSDB_CFG ${SSDB_CFG}_${REDISPORT}
    $SSDB -d ${SSDB_CFG}_${REDISPORT} -s restart &> /dev/null
}

startRedisServer(){
    if [[ ! -f "$REDIS" ]]; then
        echo "$REDIS not exists, must build it at first!"
        exit 0
    fi
    sed -i "s/port [0-9]\{1,\}/port $REDISPORT/g"  $REDIS_CFG
    $REDIS $REDIS_CFG &> /dev/null &
}

prepare_dir
cd $DIR
startSSDBServer
startRedisServer

if [ "" == "`ps -ef | grep ssdb-server | grep "_$REDISPORT"`" ]; then
    echo "ssdb-server at port $REDISPORT not start!"
    exit 0
fi

if [ "" == "`ps -ef | grep redis-server | grep ":$REDISPORT"`" ]; then
    echo "redis-server at port $REDISPORT not start!"
    exit 0
fi

echo "Start SWAP server at port $REDISPORT success!"
