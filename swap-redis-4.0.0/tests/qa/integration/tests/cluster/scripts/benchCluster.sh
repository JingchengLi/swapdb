#/bin/sh
# each master run benchmark in cluster
REDIS_CLI=./redis-cli
REDIS_BEN=./redis-benchmark
if [ $1 = "-h" ];then
    NodeIP=$2
    shift 2
else
    NodeIP=172.25.92.7
fi
if [ $1 = "-p" ];then
    NodePort=$2
    shift 2
else
    NodePort=5003
fi
mastersIPPort=`$REDIS_CLI -h $NodeIP -p $NodePort cluster nodes | grep master|awk '{print $2}'` 
mastersNode=`$REDIS_CLI -h $NodeIP -p $NodePort cluster nodes | grep master|awk '{print $1}'` 
for line in $mastersIPPort
do  
   masterIP=`echo $line | cut -d \: -f 1`
   masterPort=`echo $line | cut -d \: -f 2`
   echo $line
   $REDIS_BEN -h $masterIP -p $masterPort -r 100000000 -t set -n 10000000 -P 10
done

