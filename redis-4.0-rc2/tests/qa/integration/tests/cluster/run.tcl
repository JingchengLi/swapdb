# Cluster test suite. Copyright (C) 2014 Salvatore Sanfilippo antirez@gmail.com
# This software is released under the BSD License. See the COPYING file for
# more information.

set ::steps 0
set ::class 0
cd tests/cluster
source cluster.tcl
source ../instances.tcl
source ../../support/cluster.tcl ; # Redis Cluster client.

set ::instances_count 20 ; # How many instances we use at max.
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
        "loglevel debug"
        "cluster-require-full-coverage yes"
        "cluster-migration-barrier 1"
        "maxmemory 100M"
        "jdjr-mode yes"
        "ssdb_server_unixsocket ssdb.sock"
    }

    # if {![file exists ../../../../../src/redis-check-aof]} {
        # catch {exec make -C ../../../../../src} info
    # }
    run_tests
    cleanup
    end_tests
}

if {[catch main e]} {
    puts $::errorInfo
    if {$::pause_on_error} pause_on_error
    cleanup
    exit 1
}
