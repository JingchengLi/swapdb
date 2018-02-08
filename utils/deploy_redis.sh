#!/bin/sh

SSDB_CFG=ssdb.conf.template
REDIS_CFG=redis.conf.template
SSDB=../../../build/ssdb-server
REDIS=../../../build/redis-server

ERROR(){
    echo -e "\033[37;41m[ERROR]\033[0m $@"
}

INFO(){
    echo -e "\033[37;42m[INFO]\033[0m $@"
}

if [ -n "$1" ];then
    REDISPORT=$1
else
    REDISPORT=6379
fi
let SSDBPORT=($REDISPORT+20000)%65535

if [[ `lsof -i:$REDISPORT` > 1 || `lsof -i:$SSDBPORT` > 1 ]];then
    ERROR "redis port:$REDISPORT or ssdb port:$SSDBPORT is already used!"
    exit 1
fi

DIR=./tmp/${REDISPORT}_dir

prepare_dir() {
    mkdir -p $DIR
    cp $SSDB_CFG $REDIS_CFG $DIR
}

startSSDBServer(){
    if [[ ! -f "$SSDB" ]]; then
        ERROR "$SSDB not exists, must build it at first!"
        exit 1
    fi
    sed -i "s/ssdbport/$SSDBPORT/g" $SSDB_CFG
    sed -i "s/redisport/$REDISPORT/g" $SSDB_CFG
    mv $SSDB_CFG ${SSDB_CFG}_${REDISPORT}
    $SSDB -d ${SSDB_CFG}_${REDISPORT} -s restart &> /dev/null
}

startRedisServer(){
    if [[ ! -f "$REDIS" ]]; then
        ERROR "$REDIS not exists, must build it at first!"
        exit 1
    fi
    sed -i "s/redisport/$REDISPORT/g"  $REDIS_CFG
    $REDIS $REDIS_CFG &> /dev/null &
}

prepare_dir
cd $DIR
startSSDBServer
startRedisServer

if [ "" == "`ps -ef | grep ssdb-server | grep "_$REDISPORT"`" ]; then
    ERROR "ssdb-server at port $REDISPORT not start!"
    exit 1
fi

if [ "" == "`ps -ef | grep redis-server | grep ":$REDISPORT"`" ]; then
    ERROR "redis-server at port $REDISPORT not start!"
    exit 1
fi

INFO "Start SWAP server at port $REDISPORT success!"
