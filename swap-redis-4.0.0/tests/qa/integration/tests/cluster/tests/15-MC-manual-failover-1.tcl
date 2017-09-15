# Check the manual failover capabilities with datacenter-id set when master is running.
# One Master One Slave

# change nodePairs from 5 to 3 for this test cost much time
# set nodePairs 3
set OthersIndex 0
set MasterIndex 1
set SlaveIndex 2
set FailIndex 3
#othersId MasterIndex SlaveIndex FailIndex
# remove some equivalent cases to reduce test running time
# 1 0 0 0
# 0 0 0 1
# 0 0 1 0
# 0 1 0 0
set testcases "
                0 1 1 0
                1 0 1 0
                
                1 1 0 0
                1 1 1 1
                "
               
for {set n 0} {$n < [expr [llength $testcases]/4]} {incr n} {
    source "../tests/includes/init-tests.tcl"
    test "Create a $::nodePairs nodes cluster" {
        create_cluster $::nodePairs $::nodePairs 
    }
    set CurMaster 0
    set CurSlave $::nodePairs 

    puts "------------master:$CurMaster slave:$CurSlave--------------"
    test "Set datacenter-id others [lindex $testcases [expr $n*4+$OthersIndex]]\
    master [lindex $testcases [expr $n*4+$MasterIndex]] \
    slave [lindex $testcases [expr $n*4+$SlaveIndex]]" {
        setDatacenter_id [list $CurMaster] [lindex $testcases [expr $n*4+$MasterIndex]]
        setDatacenter_id [list $CurSlave] [lindex $testcases [expr $n*4+$SlaveIndex]]
        set nodesList ""
        for {set node 0} {$node < 10} {incr node;} {
            if {$node != $CurMaster && $node != $CurSlave} {
                lappend nodesList $node
            }
        }
        setDatacenter_id $nodesList [lindex $testcases [expr $n*4+$OthersIndex]]
        # wait for the datacenter-id broadcast to the nodes.
        after [lindex [R 0 config get cluster-node-timeout] 1]
    }

    #first time failover, second time failover force.
    for {set failoverTimes 0} {$failoverTimes < 2} {incr failoverTimes} {
    
        set failoverType [expr $failoverTimes?"FAILOVER FORCE":"FAILOVER"]
        
        test "Cluster is up" {
            assert_cluster_state ok
        }

        # test "Cluster is writable" {
            # cluster_write_test $CurMaster
        # }

        test "Instance #$CurSlave is a slave" {
            assert {[RI $CurSlave role] eq {slave}}
        }

        test "Instance #$CurSlave synced with the master" {
            wait_for_condition 1000 50 {
                [RI $CurSlave master_link_status] eq {up}
            } else {
                fail "Instance #$CurSlave master link status is not up"
            }
        }

        test "Instance #$CurSlave is slave of #$CurMaster" {
            set porttmp [get_instance_attrib redis $CurMaster port]
            assert {[lindex [R $CurSlave role] 2] == $porttmp}
        }

        set current_epoch [CI 1 cluster_current_epoch]

        set numkeys 50000
        set numops 100000
        set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis $CurMaster port]]
        catch {unset content}
        array set content {}

        if {0 == $failoverTimes} {
            test "Send CLUSTER FAILOVER to #$CurSlave, during load" {
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
                        R $CurSlave cluster FAILOVER}
                    }
                }
        } else {
            test "Send CLUSTER FAILOVER FORCE to #$CurSlave" {
                R $CurSlave cluster FAILOVER FORCE
            }
        }

        test "Wait for $failoverType" {
            wait_for_condition 1000 100 {
                [CI $CurSlave cluster_current_epoch] > $current_epoch
            } else {
                fail "No failover detected"
            }
        }

        test "Cluster should eventually be up again" {
            assert_cluster_state ok
        }

        if {0 == $failoverTimes} {
            test "Verify $numkeys keys for consistency with logical content" {
                # Check that the Redis Cluster content matches our logical content.
                foreach {key value} [array get content] {
                    assert {[$cluster lrange $key 0 -1] eq $value}
                }
            }
        }

        # test "Cluster is writable" {
            # cluster_write_test 1
        # }

        test "Instance #$CurMaster gets converted into a slave" {
            wait_for_condition 1000 50 {
                [RI $CurMaster role] eq {slave}
            } else {
                fail "Old master was not converted into slave"
            }
        }

        test "Instance #$CurSlave is now a master" {
            assert {[RI $CurSlave role] eq {master}}
        }
        
        test "Instance #$CurMaster synced with the master" {
            wait_for_condition 1000 50 {
                [RI $CurMaster master_link_status] eq {up}
            } else {
                fail "Instance #$CurMaster master link status is not up"
            }
        }
        
        test "Instance #$CurMaster Instance #$CurSlave consistency with replication" {
            assert {[R $CurSlave debug digest] eq [R $CurMaster debug digest]}
        }

        test "Instance #$CurMaster is slave of #$CurSlave" {
            set porttmp [get_instance_attrib redis $CurSlave port]
            assert {[lindex [R $CurMaster role] 2] == $porttmp}
        }
        # The second failover in same node must be auth_retry_time after first.
        #after 30000

        test "Wait auth_retry_time between two failovers" {
            after [expr [lindex [R $CurMaster config get cluster-node-timeout] 1]*2]
        }
        
        #Switch master and slave
        set CurMaster [expr $CurMaster^$CurSlave]
        set CurSlave [expr $CurMaster^$CurSlave]
        set CurMaster [expr $CurMaster^$CurSlave]
    }
}
