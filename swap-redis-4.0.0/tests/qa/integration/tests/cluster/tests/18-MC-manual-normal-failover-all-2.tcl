# Check the manual failover capabilities when switching machine room, masters are running.
# One master, Two slaves, one slave's id is same with master's, another is not.

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

#for {set CurMaster 0;set CurSlave_B 10} {$CurMaster < $::nodePairs } {incr CurMaster;incr CurSlave_B} {
foreach CurMaster $Masters CurSlave_A $Slaves_A CurSlave_B $Slaves_B {
    set NextMaster [expr ($CurMaster+1)%$::nodePairs ]
    test "Cluster is up" {
        assert_cluster_state ok
    }

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
    
    set current_epoch [CI $NextMaster cluster_current_epoch]

    set numkeys 50000
    set numops 100000
    set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis $CurMaster port]]
    
    test "Send CLUSTER FAILOVER to #$CurSlave_B, during load" {
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
            if {$j == $numops/2} {
                R $CurSlave_B cluster FAILOVER}
            }
        }
        
        test "Wait for failover" {
            wait_for_condition 1000 50 {
            [CI $NextMaster cluster_current_epoch] > $current_epoch
        } else {
            fail "No failover detected"
        }
    }

    test "Cluster should eventually be up again" {
        assert_cluster_state ok
    }

    # test "Cluster is writable" {
        # cluster_write_test $NextMaster 
    # }

    test "Instance #$CurSlave_A is still a slave" {
        assert {[RI $CurSlave_A role] eq {slave}}
    }

    test "Instance #$CurSlave_B is now a master" {
        assert {[RI $CurSlave_B role] eq {master}}
    }

    test "Verify $numkeys keys for consistency with logical content" {
        # Check that the Redis Cluster content matches our logical content.
        foreach {key value} [array get content] {
            assert {[$cluster lrange $key 0 -1] eq $value}
        }
    }

    test "Instance #$CurMaster gets converted into a slave" {
        wait_for_condition 1000 50 {
            [RI $CurMaster role] eq {slave}
        } else {
            fail "Old master was not converted into slave"
        }
    }

    test "Instance #$CurMaster and #$CurSlave_A became slaves of #$CurSlave_B" {
        set porttmp [get_instance_attrib redis $CurSlave_B port]
        assert {[lindex [R $CurMaster role] 2] == $porttmp}
        assert {[lindex [R $CurSlave_A role] 2] == $porttmp}
    }

    test "Instance #$CurMaster Instance #$CurSlave_A and Instance #$CurSlave_B consistency with replication" {
        assert {[R $CurSlave_B debug digest] eq [R $CurSlave_A debug digest]}
        assert {[R $CurSlave_B debug digest] eq [R $CurMaster debug digest]}
    }
}
