#/bin/sh
# check slave and master in cluster is consistent by dbsize.
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
