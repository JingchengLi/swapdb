#/bin/sh
# check the logfile to find whether the test pass, if pass, flushall the data of cluster
REDIS_CLI=./redis-cli
#One of cluster nodes IP Port info
if [ $1 ];then
    NodeIP=$1
else
    NodeIP=172.25.92.7
fi
if [ $2 ];then
    NodePort=$2
else
    NodePort=5003
fi
logfile=log

mastersIPPort=`$REDIS_CLI -h $NodeIP -p $NodePort cluster nodes | grep master|awk '{print $2}'` 
mastersNode=`$REDIS_CLI -h $NodeIP -p $NodePort cluster nodes | grep master|awk '{print $1}'` 
n=0
while [[ "" = ""`grep -m 1 -o -E '执行完成' $logfile` ]]
do
    sleep 10
    let ++n
    echo "继续等待 10 秒"
done
echo "等待$n次"
echo "执行完成"
mastersIPPort=`$REDIS_CLI -h $NodeIP -p $NodePort cluster nodes | grep master|awk '{print $2}'` 
mastersNode=`$REDIS_CLI -h $NodeIP -p $NodePort cluster nodes | grep master|awk '{print $1}'` 
totalSize=0
if [ "" != ""`grep -m 1 -o -E 'get错误|set错误' $logfile` ];then
    echo "检查数据出错"
    for line in $mastersIPPort
    do  
       masterIP=`echo $line | cut -d \: -f 1`
       masterPort=`echo $line | cut -d \: -f 2`
       Keys=`$REDIS_CLI -h $masterIP -p $masterPort dbsize`
       totalSize=$(expr $Keys + $totalSize)
    done
    echo "Total keys is:$totalSize"
    #exit 1
else
    echo "检查数据通过"
fi
read -t 10 -p "是否开始检查主从一致?(y/n)" yn
if [ ""$yn == "n" ];then
    echo "退出"
    exit 2
fi
echo "检查主从一致"
for line in $mastersNode
do  
    Master=`$REDIS_CLI -h $NodeIP -p $NodePort cluster nodes | grep $line |grep "master"| awk '{print $2}'`
    echo "Master $Master"
    IP=`echo $Master | cut -d \: -f 1`
    Port=`echo $Master | cut -d \: -f 2`
    masterSize=`$REDIS_CLI -h $IP -p $Port dbsize`
 
    Slave=`$REDIS_CLI -h $NodeIP -p $NodePort cluster nodes | grep $line |grep "slave"| awk '{print $2}'`
    echo "Slaves"
    for snode in $Slave
    do
        IP=`echo $snode | cut -d \: -f 1`
        Port=`echo $snode | cut -d \: -f 2`
        echo $snode 
        slaveSize=`$REDIS_CLI -h $IP -p $Port dbsize`
        if [[ $slaveSize != $masterSize ]];then
            echo "主从不一致"
            echo "master:$masterSize"
            echo "slave:$slaveSize"
            exit 3
        fi 
    done
done
read -p "是否Flushall集群数据?(y/n)" yn
if [ $yn != "y" ];then
    echo "退出"
    exit 2
fi
echo "Flushall 集群数据"
totalSize=0
for line in $mastersIPPort
do  
    masterIP=`echo $line | cut -d \: -f 1`
    masterPort=`echo $line | cut -d \: -f 2`
    echo $line
    Keys=`$REDIS_CLI -h $masterIP -p $masterPort dbsize`
    echo $Keys
    totalSize=$(expr $Keys + $totalSize)
    $REDIS_CLI -h $masterIP -p $masterPort flushall
    $REDIS_CLI -h $masterIP -p $masterPort dbsize
done
echo "Total keys is:$totalSize"
