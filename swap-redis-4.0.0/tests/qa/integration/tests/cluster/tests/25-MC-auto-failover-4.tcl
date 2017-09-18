# Check the auto failover capabilities when the master down, same machine room node can failover.
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

test "Set datacenter-id Masters 1 Slaves_A 1 Slaves_B 255" {
    setDatacenter_id $Masters 1
    setDatacenter_id $Slaves_A 1
    setDatacenter_id $Slaves_B 255
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

    test "Instance #$CurSlave_A #$CurSlave_B are slaves of #$CurMaster" {
        assert {[RI $CurSlave_A role] eq {slave}}
        assert {[RI $CurSlave_B role] eq {slave}}
        set porttmp [get_instance_attrib redis $CurMaster port]
        assert {[lindex [R $CurSlave_A role] 2] == $porttmp}
        assert {[lindex [R $CurSlave_B role] 2] == $porttmp}
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

    set current_epoch [CI $CurMaster cluster_current_epoch]
    
    test "Killing master instance #$CurMaster" {
        kill_instance redis $CurMaster
    }

    test "Cluster should eventually be up again" {
        assert_cluster_state ok
    }

    # [CI $CurMaster+1 cluster_current_epoch] > $current_epoch
    test "Wait for failover" {
        wait_for_condition 1000 50 {
            [RI $CurSlave_A role] eq {master}
        } else {
            fail "No failover detected"
        }
    }

    # test "Instance #$CurSlave_A is now a master" {
        # assert {[RI $CurSlave_A role] eq {master}}
    # }
    # test "Instance #$CurSlave_A convert to master" {
        # wait_for_condition 1000 50 {
            # [RI $CurSlave_A role] eq {master}
        # } else {
            # fail "Instance #$CurSlave_A not convert to master"
        # }
    # }

    set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis $CurSlave_A port]]
    test "Verify $numkeys keys for consistency with logical content" {
        # Check that the Redis Cluster content matches our logical content.
        foreach {key value} [array get content] {
            assert {[$cluster lrange $key 0 -1] eq $value}
        }
    }

    # test "#$CurSlave_A is writable" {
        # cluster_write_test $CurSlave_A
    # }

    test "Restarting the previously killed master node" {
        restart_instance redis $CurMaster
    }

    test "Instance #$CurMaster and #$CurSlave_B synced with the master" {
        wait_for_condition 1000 50 {
            [RI $CurMaster master_link_status] eq {up}
        } else {
            fail "Instance #$CurMaster master link status is not up"
        }
        wait_for_condition 1000 50 {
            [RI $CurSlave_B master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave_B master link status is not up"
        }
    }

    test "Instance #$CurMaster and #$CurSlave_B became slaves of #$CurSlave_A" {
        assert {[RI $CurMaster role] eq {slave}}
        assert {[RI $CurSlave_B role] eq {slave}}
        set porttmp [get_instance_attrib redis $CurSlave_A port]
        assert {[lindex [R $CurMaster role] 2] == $porttmp}
        assert {[lindex [R $CurSlave_B role] 2] == $porttmp}
    }

    test "Instance #$CurMaster Instance #$CurSlave_A and Instance #$CurSlave_B consistency with replication" {
        assert {[R $CurSlave_A debug digest] eq [R $CurSlave_B debug digest]}
        assert {[R $CurSlave_A debug digest] eq [R $CurMaster debug digest]}
    }
    # Just need run one cycle
    break
}
