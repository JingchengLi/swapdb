#!/bin/bash  
#test script: Summarize the manual failover and auto failover time.
#Slave_A is in same datacenter with Master
#Slave_B is in cross datacenter 

REDIS_CLI=../../../src/redis-cli
REDIS_CONF=redis.cfg
#Can set Nodes' info here or in redis.cfg
if [ ! -f $REDIS_CONF ];then
    Master=172.25.61.54:5020
    Slave_A=172.25.61.72:25026
    Slave_B=172.25.61.16:5008
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
    done < $REDIS_CONF
fi
echo "Master $Master"
echo "Slave_A $Slave_A "
echo "Slave_B $Slave_B"

Master_IP=`echo $Master|cut -d \: -f 1`
Master_PORT=`echo $Master|cut -d \: -f 2`
Slave_A_IP=`echo $Slave_A|cut -d \: -f 1`
Slave_A_PORT=`echo $Slave_A|cut -d \: -f 2`
Slave_B_IP=`echo $Slave_B|cut -d \: -f 1`
Slave_B_PORT=`echo $Slave_B|cut -d \: -f 2`

begin=$(date +%s)  
# Make Master is a master
if [ ""`$REDIS_CLI -h $Master_IP -p $Master_PORT role | grep slave` = "slave" ];then
    echo "Failover Master to a master."
    $REDIS_CLI -h $Master_IP -p $Master_PORT cluster failover
fi

#wait Master to be a master
declare -i n=0
while [[ ""`$REDIS_CLI -h $Master_IP -p $Master_PORT role | grep slave` = "slave" && $n -lt 30 ]]
do
    sleep 1 
    let ++n
done
endmaster=$(date +%s)  
spend=$(expr $endmaster - $begin)  
if [ $n != 0 ];then
    echo "Master manual failover cost $spend seconds"
fi

begincross1=$(date +%s)  
echo "First cross DC node manual failover"
echo "Failover Slave_B to a Master."
$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT cluster failover
declare -i n=0
while [[ ""`$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT role | grep slave` = "slave" \
    || ""`$REDIS_CLI -h $Master_IP -p $Master_PORT role | grep master` = "master" \
    || ""`$REDIS_CLI -h $Master_IP -p $Master_PORT role | grep connected` = "" \
    && $n -lt 30 ]]
do
    echo $n
    sleep 1 
    let ++n
done
$REDIS_CLI -h $Master_IP -p $Master_PORT role 
endcross1=$(date +%s)  
spendcross1=$(expr $endcross1 - $begincross1)  
echo "Cross DC node first failover time is $spendcross1 seconds"
echo "wait time between two failovers"
sleep 30
begincross2=$(date +%s)  
echo "Second cross DC node manual failover"
echo "Failover $Slave_A_IP : $Slave_A_PORT to a Master."
$REDIS_CLI -h $Slave_A_IP -p $Slave_A_PORT cluster failover
declare -i n=0
while [[ ""`$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT role | grep master` = "master" \
        || ""`$REDIS_CLI -h $Slave_A_IP -p $Slave_A_PORT role | grep slave` = "slave" \
	|| ""`$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT role | grep connected` = "" \
        && $n -lt 30 ]]
do
    echo $n
    sleep 1 
    let ++n
done
endcross2=$(date +%s)  
spendcross2=$(expr $endcross2 - $begincross2)  
echo "Cross DC node second failover time is $spendcross2 seconds"
# avercross=$(expr ( $spendcross1 + $spendcross2 ) /2 )
# echo "Average manual failover time is $avercross seconds"

echo "wait time between two failovers"
sleep 30
beginauto=$(date +%s)  
echo "Same DC node auto failover"
echo "Shutdown SlaveA ."
$REDIS_CLI -h $Slave_A_IP -p $Slave_A_PORT role
$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT role
$REDIS_CLI -h $Slave_A_IP -p $Slave_A_PORT shutdown
declare -i n=0
while [[ ""`$REDIS_CLI -h $Master_IP -p $Master_PORT role | grep slave` = "slave" \
	||""`$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT role | grep connected` = "" \
	&& $n -lt 30 ]]
do
    echo $n
    sleep 1 
    let ++n
done
endauto=$(date +%s)  
spendauto=$(expr $endauto - $beginauto)  
echo "Same DC node auto failover time is $spendauto seconds"


echo "wait time between two failovers"
sleep 30
echo "Before shutdown Master is master"
$REDIS_CLI -h $Master_IP -p $Master_PORT role
$REDIS_CLI -h $Master_IP -p $Master_PORT shutdown
sleep 1
begindowncross=$(date +%s)  
echo "Cross DC node manual failover when master is down"

echo "Before failover Slave B is slave"
$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT role
$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT cluster failover force
declare -i n=0
while [[ ""`$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT role | grep slave` = "slave" && $n -lt 30 ]]
do
    echo $n
    sleep 1 
    let ++n
done
echo "After failover Slave B become master"
$REDIS_CLI -h $Slave_B_IP -p $Slave_B_PORT role
enddowncross=$(date +%s)  
spenddowncross=$(expr $enddowncross - $begindowncross)  
echo "Cross DC node manual failover when master is down cost $spenddowncross seconds"

