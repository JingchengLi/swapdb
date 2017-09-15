# Check the failover takeover capabilities when all the masters in one machine room down.
# One master, Two slaves

# set nodePairs 5
set Masters {}
set Slaves_A {}
set Slaves_B {}
for {set n 0} {$n < $::nodePairs } {incr n} {
    lappend Masters $n
	lappend Slaves_A [expr $::nodePairs +$n]
	lappend Slaves_B [expr 2*$::nodePairs +$n]
}

source "../tests/includes/init-tests.tcl"

test "Create a $::nodePairs nodes cluster" {
    create_cluster [llength $Masters] [expr [llength $Slaves_A]+[llength $Slaves_B]]
}

puts "------------masters:$Masters Slaves_A:$Slaves_A Slaves_B:$Slaves_B--------------"

test "Set datacenter-id Masters 0 Slaves_A 0 Slaves_B 1" {
    setDatacenter_id $Masters 0
    setDatacenter_id $Slaves_A 0
    setDatacenter_id $Slaves_B 1
    # wait for the datacenter-id broadcast to the nodes.
    after [lindex [R 0 config get cluster-node-timeout] 1]
}

catch {unset content}
array set content {}

test "Cluster is up" {
    assert_cluster_state ok
}

foreach CurMaster $Masters CurSlave_A $Slaves_A CurSlave_B $Slaves_B {

    # test "Cluster Master #$CurMaster is writable" {
        # cluster_write_test $CurMaster  
    # }

    test "Instance #$CurSlave_A #$CurSlave_B are slaves" {
            assert {[RI $CurSlave_A role] eq {slave}}
            assert {[RI $CurSlave_B role] eq {slave}}
    }

    test "Instance #$CurSlave_A #$CurSlave_B synced with the master" {
        wait_for_condition 1000 50 {
            [RI $CurSlave_A master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave_A master link status is not up"
        }
        wait_for_condition 1000 50 {
            [RI $CurSlave_B master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave_B master link status is not up"
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

test "Killing all slave_A instances:$Slaves_A" {
    foreach CurSlave_A $Slaves_A {
        kill_instance redis $CurSlave_A
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

test "Send CLUSTER FAILOVER TAKEOVER to slave instances:$Slaves_B" {
    foreach CurSlave_B $Slaves_B {
        R $CurSlave_B cluster FAILOVER TAKEOVER
    }        
}

test "Cluster should eventually be up again" {
    assert_cluster_state ok
}

foreach CurMaster $Masters CurSlave_A $Slaves_A CurSlave_B $Slaves_B {

    # test "Cluster_B #$CurSlave_B is writable" {
        # cluster_write_test $CurSlave_B
    # }

    test "Instance #$CurSlave_B is now a master" {
        assert {[RI $CurSlave_B role] eq {master}}
    }

    set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis $CurSlave_B port]]
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

    test "Restarting the previously killed Slave_A node" {
        restart_instance redis $CurSlave_A 
    }

    test "Instance #$CurSlave_A return to a slave" {
        wait_for_condition 1000 50 {
            [RI $CurSlave_A role] eq {slave}
        } else {
            fail "Old slave was not return to slave"
        }
    }
   
    test "Instance #$CurMaster and #$CurSlave_A synced with the master" {
        wait_for_condition 1000 100 {
            [RI $CurMaster master_link_status] eq {up}
        } else {
            fail "Instance #$CurMaster master link status is not up"
        }
        wait_for_condition 1000 100 {
            [RI $CurSlave_A master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave_A master link status is not up"
        }
    }
    
    test "The #$CurSlave_B master has at least one slave" {
        wait_for_condition 1000 10 {
            [llength [lindex [R $CurSlave_B role] 2]] >= 1
        } else {
            fail "Instance #$CurSlave_B not own at least one slave"
        }
    }
}
