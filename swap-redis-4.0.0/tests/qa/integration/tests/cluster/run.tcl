# Cluster test suite. Copyright (C) 2014 Salvatore Sanfilippo antirez@gmail.com
# This software is released under the BSD License. See the COPYING file for
# more information.

set ::steps 0
set ::class 0
set ::loglevel debug
cd tests/cluster
source cluster.tcl
source ../instances.tcl
source ../../support/cluster.tcl ; # Redis Cluster client.

set ::instances_count 30 ; # How many instances we use at max.
set ::nodePairs 3
#       "daemonize yes"

proc main {} {
    parse_options
    spawn_instance redis $::redis_base_port $::instances_count {
        "cluster-enabled yes"
        "appendonly yes"
        "appendfsync everysec"
        "latency-monitor-threshold 10"
        "cluster-slave-validity-factor 10"
        "maxmemory-policy allkeys-lfu"
        "cluster-require-full-coverage yes"
        "cluster-migration-barrier 1"
        "maxmemory 100M"
        "ssdb_server_unixsocket ssdb.sock"
        "lfu-decay-time 0"
    }

    run_tests
    cleanup
    end_tests
}

if {[catch main e]} {
    if {$::pause_on_error} pause_on_error
    set ::failed 1;# set failed to 1 when exception avoid to rm tmp.
    cleanup
    exit 1
}
