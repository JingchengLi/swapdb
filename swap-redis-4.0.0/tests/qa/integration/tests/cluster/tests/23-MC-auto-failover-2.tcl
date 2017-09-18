# Check the basic monitoring and auto failover capabilities with datacenter-id set situations.
# One Master Two Slave, slaves are with different ids.

# set nodePairs 5
set OthersIndex 0
set MasterIndex 1
set SlaveIndex 2
set FailIndex 3
set CurMaster 0
set CurSlave_1_1 $::nodePairs 
set CurSlave_1_2 [expr $::nodePairs *2]
#OthersIndex MasterIndex SlaveIndex FailIndex
set testcases "
                0 1 0 0
                0 1 1 1
                1 0 0 1
                1 0 1 0
                
                0 0 0 1 
                0 0 1 0
                1 1 0 0
                1 1 1 1
                "
               
for {set n 0} {$n < [expr [llength $testcases]/4]} {incr n} {
    if {1} {  
        source "../tests/includes/init-tests.tcl"
        test "Create a $::nodePairs nodes cluster" {
            create_cluster $::nodePairs [expr $::nodePairs *2]
        }
        set CurMaster 0
        set CurSlave_1_1 $::nodePairs 
        set CurSlave_1_2 [expr $::nodePairs *2]
    }
    puts "------------master:$CurMaster slave_A:$CurSlave_1_1 slave_B:$CurSlave_1_2--------------"
    test "Set datacenter-id others [lindex $testcases [expr $n*4+$OthersIndex]]\
    master [lindex $testcases [expr $n*4+$MasterIndex]] \
    slave [lindex $testcases [expr $n*4+$SlaveIndex]]" {
        setDatacenter_id [list $CurMaster] [lindex $testcases [expr $n*4+$MasterIndex]]
        setDatacenter_id [list $CurSlave_1_1] [lindex $testcases [expr $n*4+$SlaveIndex]]
        setDatacenter_id [list $CurSlave_1_2] [expr [lindex $testcases [expr $n*4+$SlaveIndex]]?0:1]
        set nodesList ""
        for {set node 0} {$node < 15} {incr node;} {
            if {$node != $CurMaster && $node != $CurSlave_1_1 && $node != $CurSlave_1_2} {
                lappend nodesList $node
            }
        }
        setDatacenter_id $nodesList [lindex $testcases [expr $n*4+$OthersIndex]]
        # wait for the datacenter-id broadcast to the nodes.
        after [lindex [R 0 config get cluster-node-timeout] 1]
    }

    test "Cluster is up" {
        assert_cluster_state ok
    }

    test "Cluster is writable" {
        cluster_write_test $CurMaster
    }

    test "Instance #$CurSlave_1_1 and #$CurSlave_1_2 are slaves" {
        assert {[RI $CurSlave_1_1 role] eq {slave}}
        assert {[RI $CurSlave_1_2 role] eq {slave}}
    }

    test "Instance #$CurSlave_1_1 and #$CurSlave_1_2 are slaves of #$CurMaster" {
        set porttmp [get_instance_attrib redis $CurMaster port]
        assert {[lindex [R $CurSlave_1_1 role] 2] == $porttmp}
        assert {[lindex [R $CurSlave_1_2 role] 2] == $porttmp}
    }

    test "Instance #$CurSlave_1_1 and #$CurSlave_1_2 synced with the master" {
        wait_for_condition 1000 50 {
            [RI $CurSlave_1_1 master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave_1_1 master link status is not up"
        }
        wait_for_condition 1000 50 {
            [RI $CurSlave_1_2 master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave_1_2 master link status is not up"
        }
    }

    set current_epoch [CI 1 cluster_current_epoch]

    test "Killing one master node" {
        kill_instance redis $CurMaster
    }
        test "Wait for failover" {
            wait_for_condition 1000 50 {
                [CI 1 cluster_current_epoch] > $current_epoch
            } else {
                fail "No failover detected"
            }
        }

        test "Cluster should eventually be up again" {
            assert_cluster_state ok
        }

        test "Cluster is writable" {
            cluster_write_test 1
        }
    if {[lindex $testcases [expr $n*4+$FailIndex]] == 1} {
        test "Instance #$CurSlave_1_1 is now a master" {
            assert {[RI $CurSlave_1_1 role] eq {master}}
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
        
        test "Instance #$CurMaster and #$CurSlave_1_2 are slaves of #$CurSlave_1_1" {
            set porttmp [get_instance_attrib redis $CurSlave_1_1 port]
            assert {[lindex [R $CurMaster role] 2] == $porttmp}
            assert {[lindex [R $CurSlave_1_2 role] 2] == $porttmp}
        }
        #Switch master and slave
        set CurMaster [expr $CurMaster^$CurSlave_1_1]
        set CurSlave_1_1 [expr $CurMaster^$CurSlave_1_1]
        set CurMaster [expr $CurMaster^$CurSlave_1_1]
    } else {
        test "Instance #$CurSlave_1_2 is now a master" {
            assert {[RI $CurSlave_1_2 role] eq {master}}
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
        
        test "Instance #$CurMaster and #$CurSlave_1_1 are slaves of #$CurSlave_1_2" {
            set porttmp [get_instance_attrib redis $CurSlave_1_2 port]
            assert {[lindex [R $CurMaster role] 2] == $porttmp}
            assert {[lindex [R $CurSlave_1_1 role] 2] == $porttmp}
        }
        #Switch master and slave
        set CurMaster [expr $CurMaster^$CurSlave_1_2]
        set CurSlave_1_2 [expr $CurMaster^$CurSlave_1_2]
        set CurMaster [expr $CurMaster^$CurSlave_1_2]
    }
    
    test "Instance #$CurSlave_1_1 and #$CurSlave_1_2 synced with the master" {
        wait_for_condition 1000 50 {
            [RI $CurSlave_1_1 master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave_1_1 master link status is not up"
        }
        wait_for_condition 1000 50 {
            [RI $CurSlave_1_2 master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave_1_2 master link status is not up"
        }
    }
    
    test "Instance #$CurSlave_1_1 and #$CurSlave_1_2 are slaves of #$CurMaster" {
        set porttmp [get_instance_attrib redis $CurMaster port]
        assert {[lindex [R $CurSlave_1_1 role] 2] == $porttmp}
        assert {[lindex [R $CurSlave_1_2 role] 2] == $porttmp}
    }
}
