#!/bin/bash

####################################
# play.sh - Redis Cluster Playground
#  - requires on your path:
#    - tmux
#    - redis-server
#    - redis-trib.rb
#
#  - run with no arguments for help
####################################

base=`dirname $0`
nodeConfigDir=$base/nodes
tmuxScript=$base/tmuxPlayground.sh
nodeMap=$base/nodemap
loglevel=debug

PATH=$PATH:~/repos/redis/src/

verboseRun() {
    echo Running command: $@
    $@
}

spawnNode() {
    IP=$1
    PORT=$2
    nodeConfigBase=$nodeConfigDir/node-$PORT
    nodeConfigPath=$nodeConfigBase/node-$PORT.conf

    mkdir -p $nodeConfigBase

    echo "Removing existing node config $nodeConfigPath..."
    rm -f $nodeConfigPath
    rm -f $nodeConfigBase/*.rdb

    echo "Spawning Redis Cluster node on port $PORT..."

    # Add this ip/port combination to the current nodemap
    echo $IP:$PORT >> $nodeMap

    cd $nodeConfigBase

    # Cluster nodes are still saving dump.rdb even with save disabled.
    verboseRun redis-server --bind $IP --port $PORT --save \'\' --cluster-enabled yes --cluster-config-file $nodeConfigPath --loglevel $loglevel
}

generateLimitedPorts() {
    MAX_PORTS=$1
    current_count=1
    PORTS=7{0..9}{0..9}{0..9}  # From 7000 up to 7999 = 1000 possible ports
    for port in $(eval echo $PORTS); do
        echo $port
        current_count=$((current_count + 1))
        if [[ $current_count -gt $MAX_PORTS ]]; then
            return
        fi
    done
}

spawnNodes() {
    NODE_COUNT=$1
    NODE_TYPE=$2
    HIGH_OCTET=$3

    rm -f $tmuxScript
    rm -f $nodeMap

    # Let's bundle all the cluster nodes under one tmux session
    echo tmux new-session -d -s clusterNodes -n empty >> $tmuxScript

    for port in `generateLimitedPorts $NODE_COUNT`; do
        if [[ $NODE_TYPE == port ]]; then
            echo tmux new-window -t clusterNodes -n cl$port \"$PWD/$0 node $port\" >> $tmuxScript
        elif [[ $NODE_TYPE == ipnode ]]; then
            chosenOctet=$((RANDOM % $HIGH_OCTET + 1))  # range of [1, $HIGH_OCTET]
            ip=127.0.0.$chosenOctet
            echo tmux new-window -t clusterNodes -n cl$port \"$PWD/$0 ip-node $ip $port\" >> $tmuxScript
        fi
    done

    # Remove our empty shell window from the new-session call above
    echo tmux kill-window -t empty >> $tmuxScript

    # Now, with all our nodes running in the background, attach to them
    echo tmux attach -t clusterNodes >> $tmuxScript

    echo "Wrote $NODE_COUNT node tmux script to $tmuxScript"
    verboseRun sh $tmuxScript

}

checkNode() {
    PORT=$1
    echo "Checking Redis Cluster node on port $PORT..."
    verboseRun redis-trib.rb check 127.0.0.1:$PORT
}

checkNodes() {
    NODE_COUNT=$1
    for port in `generateLimitedPorts $NODE_COUNT`; do
        checkNode $port
    done
}

createCluster() {
    NODE_COUNT=$1
    REPLICAS=$2
    createArgs=""
    if [[ $REPLICAS != "masters" ]]; then
        replicaArgs="--replicas $REPLICAS"
    fi
    for port in `generateLimitedPorts $NODE_COUNT`; do
        createArgs="$createArgs 127.0.0.1:$port"
    done
    verboseRun redis-trib.rb create $replicaArgs $createArgs
}

createClusterFromNodeMap() {
    createArgs=""
    REPLICAS=$1
    if [[ $REPLICAS != "masters" ]]; then
        replicaArgs="--replicas $REPLICAS"
    fi
    for line in `cat $nodeMap`; do
        parts=(${line//:/ })
        ip=${parts[0]}
        port=${parts[1]}
        createArgs="$createArgs $ip:$port"
    done
    verboseRun redis-trib.rb create $replicaArgs $createArgs
}

blockIfNotExists() {
    WHAT=$1
    which $WHAT > /dev/null
    if [[ $? == 1 ]]; then
        echo "Sorry, but $WHAT isn't on your path."
        echo "Install $WHAT or set your PATH to include $WHAT."
        exit 1
    fi
}

addLocalIps() {
    OCTET_RANGE={2..254}
    case `uname -s` in
        Linux)
            for n in $(eval echo $OCTET_RANGE); do
                verboseRun sudo /sbin/ip addr add 127.0.0.$n/8 dev lo
            done
            ;;
        Darwin)
            for n in $(eval echo $OCTET_RANGE); do
                verboseRun sudo /sbin/ifconfig lo0 add 127.0.0.$n
            done
            ;;
    esac
}

blockIfNotExists tmux
blockIfNotExists redis-server
blockIfNotExists redis-trib.rb

case "$1" in
    node)
        PORT=$2
        spawnNode 127.0.0.1 $PORT
        ;;
    ip-node)
        IP=$2
        PORT=$3
        spawnNode $IP $PORT
        ;;
    nodes)
        NODE_COUNT=$2
        spawnNodes $NODE_COUNT port
        ;;
    ip-nodes)
        NODE_COUNT=$2
        HIGHEST_LAST_OCTET=$3
        spawnNodes $NODE_COUNT ipnode $HIGHEST_LAST_OCTET
        ;;
    check)
        PORT=$2
        checkNode $PORT
        ;;
    checks)
        NODE_COUNT=$2
        checkNodes $NODE_COUNT
        ;;
    create)
        NODE_COUNT=$2
        createCluster $NODE_COUNT masters
        ;;
    create-with-replicas)
        NODE_COUNT=$2
        REPLICA_COUNT=$3
        createCluster $NODE_COUNT $REPLICA_COUNT
        ;;
    create-from-nodemap)
        createClusterFromNodeMap masters
        ;;
    create-from-nodemap-with-replicas)
        REPLICA_COUNT=$2
        createClusterFromNodeMap $REPLICA_COUNT
        ;;
    add-local-ips)
        addLocalIps
        ;;
    *)
        echo "Primary Commands:"
        echo "$0 [node PORT | nodes NODE_COUNT |  check PORT | checks NODE_COUNT | create NODE_COUNT | create-with-replicas NODE_COUNT REPLICA_COUNT ]"
        echo "Advanced Commands:"
        echo "$0 [ip-node IP PORT | ip-nodes NODE_COUNT HIGHEST-LOCALHOST-LAST-OCTET | create-from-nodemap | create-from-nodemap-with-replicas REPLICA_COUNT | add-local-ips]"
        ;;
esac
