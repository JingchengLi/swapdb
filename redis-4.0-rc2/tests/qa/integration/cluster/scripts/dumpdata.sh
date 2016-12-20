# !/bin/bash  
# test script: Dump data into cluster, verify the data integrity
# Slave_A is in same datacenter with Master
# Slave_B is in cross datacenter 

REDIS_CLI=../../../src/redis-cli
REDIS_CONF=redis.cfg
begin=$(date +%s)  
# Can set Nodes' info here or in redis.cfg
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

  
echo "key $key length before:"
$REDIS_CLI -h $Master_IP -p $Master_PORT -c llen $key
for ((i=0; i<$clients;))  
do  
    $REDIS_CLI -h $Slave_A_IP -p $Slave_A_PORT -c -r $count lpush $key abcdefg 1> /dev/null &
    i=$(expr $i + 1)  
done  

# for ((i=0; i<$clients;))  
# do  
    # $REDIS_CLI -h $Master_IP -p $Master_PORT -c -r $count lpush $key abcdefg 1> /dev/null &
    # i=$(expr $i + 1)  
# done  
wait  
echo "key $key length at last:"
$REDIS_CLI -h $Master_IP -p $Master_PORT -c llen $key
end=$(date +%s)  
spend=$(expr $end - $begin)  
echo "cost $spend seconds"
