#/bin/sh
# Send command to masters in cluster
REDIS_CLI=./redis-cli
#One of cluster nodes
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
totalSize=0
echo $*
for line in $mastersIPPort
do  
    masterIP=`echo $line | cut -d \: -f 1`
    masterPort=`echo $line | cut -d \: -f 2`
    echo $line
    ret=`$REDIS_CLI -h $masterIP -p $masterPort $*`
    echo $ret
    if [ $1 = "dbsize" ];then
        totalSize=$(expr $totalSize + $ret)
    fi
done
if [ $totalSize != 0 ];then
    echo "total dbsize is $totalSize"
fi
