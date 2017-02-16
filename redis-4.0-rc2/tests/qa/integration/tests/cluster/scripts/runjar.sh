#/bin/sh
# run jar dump data and checklog
zkNode=172.23.103.243:2181
appName=zj_jar_5
keysNO=10000000
valueLen=1
threads=100

#start dump data to cluster
nohup java -jar redis-stress-1.0-SNAPSHOT-shaded.jar $zkNode $appName "zj_test_1w" $keysNO $valueLen $threads &>log &
sh checkLog.sh $*
