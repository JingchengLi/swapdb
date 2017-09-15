# Check the failover takeover capabilities when all the masters in one machine room down.
# One master, One slave

# set nodePairs 5
set SlaveNo 1
set Masters {}
set Slaves {}
for {set n 0} {$n < $::nodePairs } {incr n} {
    lappend Masters $n
    for {set m 0} {$m < $SlaveNo} {incr m} {
        lappend Slaves [expr ($m+1)*$::nodePairs +$n]
    }
}
set Slaves [lsort $Slaves]

source "../tests/includes/init-tests.tcl"

test "Create a $::nodePairs nodes cluster" {
    create_cluster [llength $Masters] [llength $Slaves]
}

puts "------------masters:$Masters slaves:$Slaves--------------"

test "Set datacenter-id masters 0 slaves 1" {
    setDatacenter_id $Masters 0
    setDatacenter_id $Slaves 1
    # wait for the datacenter-id broadcast to the nodes.
    after [lindex [R 0 config get cluster-node-timeout] 1]
}

catch {unset content}
array set content {}

test "Cluster is up" {
    assert_cluster_state ok
}

foreach CurMaster $Masters CurSlave $Slaves {

    # test "Cluster Master #$CurMaster is writable" {
        # cluster_write_test $CurMaster  
    # }

    test "Instance #$CurSlave is slave" {
        assert {[RI $CurSlave role] eq {slave}}
    }

    test "Instance #$CurSlave synced with the master" {
        wait_for_condition 1000 50 {
            [RI $CurSlave master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave master link status is not up"
        }
    }

    set numkeys 50000
    set numops 100000
    set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis $CurMaster port]]
    
    test "Load data into cluster" {
        for {set j 0} {$j < $numops} {incr j} {
            # Write random data to random list.
            set listid [randomInt $numkeys]
            set key "key:$listid"
            set ele [randomValue]
            # We write both with Lua scripts and with plain commands.
            # This way we are able to stress Lua -> Redis command invocation
            # as well, that has tests to prevent Lua to write into wrong
            # hash slots.
            #if {$listid % 2} {
                $cluster rpush $key $ele
            #} else {
            #   $cluster eval {redis.call("rpush",KEYS[1],ARGV[1])} 1 $key $ele
            #}
            lappend content($key) $ele

            if {($j % 1000) == 0} {
                puts -nonewline W; flush stdout
            }
        }
    }
}

test "Killing all master instances:$Masters" {
    foreach CurMaster $Masters {
        kill_instance redis $CurMaster
    }
}

test "Cluster should eventually be fail" {
    assert_cluster_state fail 
}
test "Send CLUSTER FAILOVER TAKEOVER to slave instances:$Slaves" {
    foreach CurSlave $Slaves {
        R $CurSlave cluster FAILOVER TAKEOVER
    }        
}

test "Cluster should eventually be up again" {
    assert_cluster_state ok
}

foreach CurMaster $Masters CurSlave $Slaves {
    # test "Cluster is writable" {
        # cluster_write_test $CurSlave
    # }

    test "Instance #$CurSlave is now a master" {
        assert {[RI $CurSlave role] eq {master}}
    }

    set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis $CurSlave port]]
    test "Verify $numkeys keys for consistency with logical content" {
        # Check that the Redis Cluster content matches our logical content.
        foreach {key value} [array get content] {
            assert {[$cluster lrange $key 0 -1] eq $value}
        }
    }

    test "Restarting the previously killed master node" {
        restart_instance redis $CurMaster
    }

    test "Instance #$CurMaster gets converted into a slave" {
        wait_for_condition 1000 50 {
            [RI $CurMaster role] eq {slave}
        } else {
            fail "Old master was not converted into slave"
        }
    }

    test "Instance #$CurMaster synced with the master" {
        wait_for_condition 1000 50 {
            [RI $CurMaster master_link_status] eq {up}
        } else {
            fail "Instance #$CurMaster master link status is not up"
        }
    }

    test "The #$CurSlave master has one slave" {
        wait_for_condition 1000 10 {
            [llength [lindex [R $CurSlave role] 2]] == 1
        } else {
            fail "Instance #$CurSlave not own one slave"
        }
    }

    test "Instance #$CurSlave is slave of #$CurSlave" {
        set porttmp [get_instance_attrib redis $CurSlave port]
        assert {[lindex [R $CurMaster role] 2] == $porttmp}
    }    
}
