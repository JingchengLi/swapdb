#!/bin/bash
REDIS_CONF_TEMPLATE=redis_xxxx.conf.template
REDIS_INIT_SHELL_TEMPLATE=redis_xxxx_init_script.template
PIDFILE=/var/run/wy/redis_$1.pid

used_mem=`free -m | grep Mem | awk '{print  $3}'`
total_mem=`free -m | grep Mem | awk '{print  $2}'`
need_mem=`expr $used_mem + $2`
usage_mem=`echo "scale=2;$need_mem/$total_mem"|bc`
usage_mem_percent=`echo "0"$usage_mem \* 100|bc`
free_mem=`expr $total_mem - $need_mem`

HOSTNAME=`hostname`

if [ $free_mem -lt 5200 ];then
    flag=`expr "0"$usage_mem \> "0.90"`
    if [ $need_mem -ge $total_mem ] ;then
        flag=1
    fi
    
    if [ "$flag" == "1" ] ;then
        echo "The mem Utilization(total_mem:$total_mem used_mem:$used_mem redis_pre_mem:$2) on $HOSTNAME is over 90%. "
        echo "this host had pre use(used_mem:$used_mem redis_pre_mem:$2) $usage_mem_percent"%" memory"
        exit 0
    fi
fi


if [ ! -d "/opt/wy/redis_data/$1" ];then
        mkdir -p /opt/wy/redis_data/$1
fi

if [ ! -d "/opt/wy/$3/$1" ];then
        mkdir -p /opt/wy/$3/$1
fi

if [ ! -d "/opt/wy/$3/crontab" ];then
        mkdir -p /opt/wy/$3/crontab
fi

if [ ! -d "/etc/init.d" ];then
        mkdir -p /etc/init.d
fi

if [ ! -d "/var/run/wy" ];then
	mkdir -p /var/run/wy
fi

if [ ! -d "/var/log/wy/" ];then
	mkdir -p /var/log/wy/
fi

if [ ! -d "/etc/init.d/redis_$1" ];then
        true > /etc/init.d/redis_$1
        chmod 777 /etc/init.d/redis_$1
fi

if [ -f "/opt/wy/$3/$1/$1.conf" ];then
	true > /opt/wy/$3/$1/$1.conf
fi

awk '{
	if ( $1~/^port$/ || 
	    $1~/^pidfile$/ ||
	    $1~/^logfile$/ ||
	    $1~/^dir$/ ||
	    $1~/^appendfilename$/ ||
	    $1~/^dbfilename$/ ||
	    $1~/^cluster-config-file$/) 
	    gsub("xxxx","'$1'", $0);
	    if ( ($0!~/^#/ || $1~/^#####/ ) && $0!~/^(\t|\ )*$/) print $0;
	}' $REDIS_CONF_TEMPLATE > /opt/wy/$3/$1/$1.conf
	
sed -i "s/yyyy/$3/g" /opt/wy/$3/$1/$1.conf

sed -i "s/zzzz/$4/g" /opt/wy/$3/$1/$1.conf

awk -F'=' '{
	if ( $1~/^REDISPORT$/) 
	    gsub("xxxx","'$1'", $0);
	    if ( $0!~/^(\t|\ )*$/) print $0;
	}' $REDIS_INIT_SHELL_TEMPLATE > /etc/init.d/redis_$1

sed -i "s/yyyy/$3/g" /etc/init.d/redis_$1

sed -i "s/zzzz/$4/g" /etc/init.d/redis_$1
 
touch /opt/wy/$3/$1/nodes_$1.conf

/etc/init.d/redis_$1 start

if [ -f "/opt/wy/$3/$1/$1.conf" ];then
    linux_release="`cat /etc/issue | awk '{printf $1}'`"
    if [ $linux_release != 'Ubuntu' ];then
        chkconfig  --add redis_$1
    fi
fi

PROGRAM=/opt/wy/$3/crontab/crontab_$1.sh
if [ ! -f $PROGRAM ];then
       true > $PROGRAM
fi
cat crontab_xxxx.template > $PROGRAM
sed -i "s/xxxx/$1/g" $PROGRAM
chmod 777 $PROGRAM
CRONTAB_CMD="* * * * * $PROGRAM"
(crontab -l 2>/dev/null | grep -Fv $PROGRAM; echo "$CRONTAB_CMD") | crontab - 
COUNT=`crontab -l | grep $PROGRAM | grep -v "grep"|wc -l ` 
if [ $COUNT -lt 1 ]; then 
        echo "fail to add crontab $PROGRAM"
fi

if [ ! -f "/opt/wy/$3/start_all.sh" ];then
        touch /opt/wy/$3/start_all.sh
        chmod 777 /opt/wy/$3/start_all.sh
        echo "#!/bin/bash" >> /opt/wy/$3/start_all.sh
fi

grepbuf=`grep "/etc/init.d/redis_$1 start" /opt/wy/$3/start_all.sh`
if [ "$grepbuf" = "" ];then
        echo "/etc/init.d/redis_$1 start" >> /opt/wy/$3/start_all.sh
fi

if [ ! -f "/opt/wy/$3/stop_all.sh" ];then
        touch /opt/wy/$3/stop_all.sh
        chmod 777 /opt/wy/$3/stop_all.sh
        echo "#!/bin/bash" >> /opt/wy/$3/stop_all.sh
fi

grepbuf=`grep "/etc/init.d/redis_$1 stop" /opt/wy/$3/stop_all.sh`
if [ "$grepbuf" = "" ];then
        echo "/etc/init.d/redis_$1 stop" >> /opt/wy/$3/stop_all.sh
fi

if [ ! -f "/opt/wy/$3/restart_all.sh" ];then
        touch /opt/wy/$3/restart_all.sh
        chmod 777 /opt/wy/$3/restart_all.sh
        echo "#!/bin/bash" >> /opt/wy/$3/restart_all.sh
fi

grepbuf=`grep "/etc/init.d/redis_$1 restart" /opt/wy/$3/restart_all.sh`
if [ "$grepbuf" = "" ];then
        echo "/etc/init.d/redis_$1 restart" >> /opt/wy/$3/restart_all.sh
fi

timeout=10
check_count=3
flag_stop=0
while :
do
    process_status="`ps -ef | grep redis-server | grep \" $4:$1 \"`"
    echo "process_status:$process_status"
    if [ "$process_status" != "" ];then
        while :
        do
            pdxredis="`netstat -anp | grep redis-server | grep \":$1 \"`"
            if [ "$pdxredis" != "" ]
            then
                echo "start redis ok"
                let flag_stop=1
                break;
            else
                if [ $timeout -eq 0 ];then
                    echo "start redis fail"
                    let flag_stop=1
                    break;
                fi
            fi
            sleep 1
            let timeout=$timeout-1
        done
        if [ $flag_stop -eq 1 ];then
            break;
        fi
    else
        if [ $check_count -eq 0 ];then
            echo "start redis fail"
            break;
        fi
    fi
    sleep 0.05
    let check_count=$check_count-1
done
