#!/bin/bash  
#test script: Failover when dump data into the cluster, verify the data integrity
#Slave_A is in same datacenter with Master
#Slave_B is in cross datacenter 


REDIS_CONF=redis.cfg
begin=$(date +%s)  
#Can set Nodes' info here or in redis.cfg
if [ ! -f $REDIS_CONF ];then
    Master=172.25.61.54:5020
    Slave_A=172.25.61.72:25026
    Slave_B=172.25.61.16:5008
    # Start how many clients to dump data
    clients=100
    # How many commands one client will exec
    count=200000
    # Make sure the key is what you want to set, decided by which node you want to set.
    key=4
    REDIS_CLI=./redis-cli
else
    while read LINE
    do
        if [ "" != ""`echo $LINE | grep -oE "^Master"` ];then
            Master=`echo $LINE | awk '{print $2}'`
        fi

        if [ "" != ""`echo $LINE | grep -oE "^Slave_A"` ];then
            Slave_A=`echo $LINE | awk '{print $2}'`
        fi

        if [ "" != ""`echo $LINE | grep -oE "^Slave_B"` ];then
            Slave_B=`echo $LINE | awk '{print $2}'`
        fi

        if [ "" != ""`echo $LINE | grep -oE "^clients"` ];then
            clients=`echo $LINE | awk '{print $2}'`
        fi

        if [ "" != ""`echo $LINE | grep -oE "^count"` ];then
            count=`echo $LINE | awk '{print $2}'`
        fi

        if [ "" != ""`echo $LINE | grep -oE "^key"` ];then
            key=`echo $LINE | awk '{print $2}'`
        fi

        if [ "" != ""`echo $LINE | grep -oE "^REDIS_CLI"` ];then
            REDIS_CLI=`echo $LINE | awk '{print $2}'`
        fi
    done < $REDIS_CONF
fi
echo "Master $Master"
echo "Slave_A $Slave_A "
echo "Slave_B $Slave_B"
echo "clients $clients;count $count;key $key"

Master_IP=`echo $Master|cut -d \: -f 1`
Master_PORT=`echo $Master|cut -d \: -f 2`
Slave_A_IP=`echo $Slave_A|cut -d \: -f 1`
Slave_A_PORT=`echo $Slave_A|cut -d \: -f 2`
Slave_B_IP=`echo $Slave_B|cut -d \: -f 1`
Slave_B_PORT=`echo $Slave_B|cut -d \: -f 2`

# Make Master is a master
if [ ""`$REDIS_CLI -h $Master_IP -p $Master_PORT role | grep slave` = "slave" ];then
    echo "Failover $Master_IP : $Master_PORT to a Master."
    $REDIS_CLI -h $Master_IP -p $Master_PORT cluster failover
fi

#wait Master to be a master
n=0
while [[ ""`$REDIS_CLI -h $Master_IP -p $Master_PORT role | grep slave` = "slave" && $n < 30 ]]
do
    sleep 1 
    let ++n
done

clients=100
count=200000
key=4
  
echo "Same datacenter failover test"
for ((i=0; i<$clients;))  
do  
    $REDIS_CLI -h $Master_IP -p $Master_PORT -c -r $count lpush $key abcdefg 1> /dev/null &
    #$REDIS_CLI -h $Master_IP -p $Master_PORT -c lpush 4 $header"$i" 1> /dev/null &
    i=$(expr $i + 1)  
done  
sleep `expr $count / 10000`
echo "key $key length before failover:"
$REDIS_CLI -h $Master_IP -p $Master_PORT -c llen $key
echo "send cluster failover to slave:"$Slave_A_IP":"$Slave_A_PORT"."
$REDIS_CLI -h $Slave_A_IP -p $Slave_A_PORT cluster failover
wait  
echo "key $key length at last:"
$REDIS_CLI -h $Master_IP -p $Master_PORT -c llen $key
if [ ""`$REDIS_CLI -h $Slave_A_IP -p $Slave_A_PORT role | grep slave` = "master" ];then
    echo $Slave_A_IP":"$Slave_A_PORT" become master."
else
    echo "Failover fail for $Slave_A_IP : $Slave_A_PORT."
fi
end=$(date +%s)  
spend=$(expr $end - $begin)  
echo "cost $spend seconds"

echo ""
echo ""

begin=$(date +%s)  
echo "Cross datacenter failover test"
for ((i=0; i<$clients;))  
do  
    $REDIS_CLI -h $Master_IP -p $Master_PORT -c -r $count lpush $key abcdefg 1> /dev/null &
    #$REDIS_CLI -h $Master_IP -p $Master_PORT -c lpush 4 $header"$i" 1> /dev/null &
    i=$(expr $i + 1)  
done  
sleep `expr $count / 10000`
echo "key $key length before failover:"
$REDIS_CLI -h $Master_IP -p $Master_PORT -c llen $key
echo "send cluster failover to slave:"$Slave_B_IP":"$Slave_B_PORT"."
$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT cluster failover
wait  
sleep 1  
echo "key $key length at last:"
$REDIS_CLI -h $Master_IP -p $Master_PORT -c llen $key
if [ ""`$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT role | grep slave` = "master" ];then
    echo $Slave_B_IP":"$Slave_B_PORT" become master."
else
    echo "Failover fail for $Slave_B_IP : $Slave_B_PORT."
fi
end=$(date +%s)  
spend=$(expr $end - $begin)  
echo "cost $spend seconds"

exit 0
$REDIS_CLI -h $Slave_A_IP -p $Slave_A_PORT flushall
$REDIS_CLI -h $Master_IP -p $Master_PORT flushall
#
