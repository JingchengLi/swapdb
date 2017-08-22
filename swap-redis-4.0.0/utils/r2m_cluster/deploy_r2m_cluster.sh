#!/bin/sh

CONFIG_DIR=/etc/redis
CLUSTER_TEMPLATE_CONF=redis_xxxx.conf.template
CLUSTER_INSTANCES_CSV_FILE=cluster_instances.csv
CLUSTER_INSTANCES_CSV_FILE_DEAULT=cluster_instances.csv.default

prepare_dir() {
    mkdir -p $CONFIG_DIR
    mkdir -p /var/run/wy/
    mkdir -p /export/log/wy
    mkdir -p /export/wy/redis_data
}

clear_redis() {
    PORT=$1
    ../../src/redis-cli -p $PORT shutdown nosave
    rm /var/run/wy/redis_$PORT.pid
    rm /export/log/wy/redis_$PORT.log
    rm /export/wy/redis_data/$PORT -rf
}

#check for root user
if [ "$(id -u)" -ne 0 ] ; then
    echo "You must run this script as root. Sorry!"
    exit 1
fi

if [ $# -eq 0 ]; then
    if [ ! -f  $CLUSTER_TEMPLATE_CONF ] ; then
        echo "cluster instance template conf file don't exist."
        exit 1
    fi
    
    if [ ! -f $CLUSTER_INSTANCES_CSV_FILE ] ; then
        echo "\033[31m $CLUSTER_INSTANCES_CSV_FILE not found, use default config file:$CLUSTER_INSTANCES_CSV_FILE_DEAULT\n\033[0m"
        CLUSTER_INSTANCES_CSV_FILE=$CLUSTER_INSTANCES_CSV_FILE_DEAULT
        if [ ! -f $CLUSTER_INSTANCES_CSV_FILE ] ; then
            echo "cluster instances config file don't exist."
            exit 1
        fi
    fi
    
    is_cluster_enabled=`grep "cluster-enabled" $CLUSTER_TEMPLATE_CONF | awk '{printf $2}'`
    if [ "$is_cluster_enabled" != "yes" ] ; then
        echo "cluster_enabled option is not yes!"
        exit 1
    fi
    
    awk 'BEGIN {print "R2M cluster instances config is:\n\033[33m>>>>>>>>>>>>>>>>>>>>"} {
        if($0!~/^#/)print "\033[33m"$0
        } END {print "\033[33m<<<<<<<<<<<<<<<<<<<<<\nplease confirm these!!!\033[0m"}' $CLUSTER_INSTANCES_CSV_FILE
    
    read -p "CONTINUE(yes/no):" input
    if [ "$input" != "yes" ] ; then
        echo "please modify cluster_instances.csv file to define your config at first."
        exit 1
    fi
    
    
    memory_config=`awk '{if ($0~/^maxmemory/) {printf $2; exit} else next}' $CLUSTER_INSTANCES_CSV_FILE`
    
    if [ "" != `echo $memory_config | grep -oE "^[0-9]+(mb|MB|M|m)$"` ];then
        number=`echo $memory_config | grep -oE "[0-9]+"`
        memory_size=$(($number * 1024 * 1024)) 
    else
        echo "invalid maxmemory. example:100mb|MB|M|m"
        exit 1
    fi
    
    master_cnt=`awk '{if ($0~/^master_cnt/) {printf $2; exit} else next}' $CLUSTER_INSTANCES_CSV_FILE`
    test_str=`echo $master_cnt | grep -oE "^[0-9]+$"`
    if [ "" != "$test_str" ]; then
        if [ $master_cnt -gt 20 ]; then
            echo "too many nodes greater than 20."
            exit 1
        fi
    else
        echo "invalid master count."
        exit 1
    fi
    echo "master_cnt:$master_cnt"
    if [ $master_cnt -lt 3 ];then
        echo "redis cluster must have at least 3 master instances."
        exit 1
    fi
    
    replicas=`awk '{if ($0~/^replicas/) {printf $2; exit} else next}' $CLUSTER_INSTANCES_CSV_FILE`
    test_str=`echo $replicas | grep -oE "^[0-9]+$"`
    if [ "" != "$test_str" ]; then
        if [ $replicas -gt 3 ]; then
            echo "too many replicas greater than 3."
            exit 1
        fi
    else
        echo "invalid replicas count."
        exit 1
    fi
    echo "replicas:$replicas"
    
    nodes_cnt=`awk 'BEGIN {a=0} {if ($0~/^redis/) a++} END {printf a}' $CLUSTER_INSTANCES_CSV_FILE`
    if [ $(($master_cnt*($replicas+1))) -ne $(($nodes_cnt)) ]; then
        echo "redis nodes count is not match! master count($master_cnt),replicas($replicas)."\
            " you should have $(($master_cnt*($replicas+1))) redis instances. but there are $(($nodes_cnt)) instances."
        exit 1
    fi
    
    if [ ! -f "../../src/redis-server" ];then
        echo "../../src/redis-server not found! please compile redis at first."
        exit 1
    fi
    echo "nodes_cnt:$nodes_cnt"
    
    prepare_dir
    
    while read LINE
    do
        # redis 127.0.0.1:7000
        test_str=`echo $LINE | grep -oE "^redis"`
        if [ "" != "$test_str" ]; then
            IP=`echo $LINE | awk '{print $2}' | awk -F':' '{printf $1}'`
            PORT=`echo $LINE | awk '{print $2}' | awk -F':' '{printf $2}'`
            test_str=`echo $IP | grep -oE "^[.0-9]+$"`
            if [ ""x = "$test_str"x ];then
                echo "Invalid ip argument:$IP"
                exit 1
            fi
            test_str=`echo $PORT | grep -oE "^[0-9]+$"`
            if [ ""x = "$test_str"x ];then
                echo "Invalid port argument:$PORT"
                exit 1
            fi
            clear_redis $PORT
        fi
    done < $CLUSTER_INSTANCES_CSV_FILE
    
    instances_str=""
    while read LINE
    do
        # redis 127.0.0.1:7000
        test_str=`echo $LINE | grep -oE "^redis"`
        if [ "" != "$test_str" ]; then
            IP=`echo $LINE | awk '{print $2}' | awk -F':' '{printf $1}'`
            PORT=`echo $LINE | awk '{print $2}' | awk -F':' '{printf $2}'`
            test_str=`echo $IP | grep -oE "^[.0-9]+$"`
            if [ ""x = "$test_str"x ];then
                echo "Invalid ip argument:$IP"
                exit 1
            fi
            test_str=`echo $PORT | grep -oE "^[0-9]+$"`
            if [ ""x = "$test_str"x ];then
                echo "Invalid port argument:$PORT"
                exit 1
            fi
    
            awk '{
                if ($0~/xxxx/) {
                    gsub("xxxx","'$PORT'",$0)
                }
                if ($0~/yyyy/) {
                    gsub("yyyy","'$memory_size'",$0)
                }
                if ($0~/zzzz/) {
                    gsub("zzzz","'$IP'",$0)
                }
                print $0
            }' $CLUSTER_TEMPLATE_CONF > $CONFIG_DIR/r2m_$PORT.conf
            instances_str=$instances_str" "$IP":"$PORT
            echo "../../src/redis-server  $CONFIG_DIR/r2m_$PORT.conf"
            mkdir -p /export/wy/redis_data/$PORT
            ../../src/redis-server  $CONFIG_DIR/r2m_$PORT.conf
        fi
    done < $CLUSTER_INSTANCES_CSV_FILE
    
    echo "../../src/redis-trib.rb create --replicas $replicas $instances_str"
    ../../src/redis-trib.rb create --replicas $replicas $instances_str
else
    case $1 in
        clear)
            if [ $# -lt 2 ]; then
                echo "please specify redis port to clear."
            fi
            for p in "$@"
            do
                clear_redis $p
            done
            ;;
    esac
fi
