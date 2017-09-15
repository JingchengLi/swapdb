# Check the basic monitoring and auto failover capabilities with datacenter-id set situations.
# One Master Two Slave, slaves are with same id.

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
        setDatacenter_id [list $CurSlave_1_2] [lindex $testcases [expr $n*4+$SlaveIndex]]
        set nodesList ""
        for {set node 0} {$node < [expr $::nodePairs*3]} {incr node;} {
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

    if {[lindex $testcases [expr $n*4+$FailIndex]] == 1} {
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

        set NewMaster -1
        set StillSlave -1
	if { [RI $CurSlave_1_1 role]  eq {master}} {
        set NewMaster $CurSlave_1_1
	set StillSlave $CurSlave_1_2
	} else {
        set NewMaster $CurSlave_1_2
	set StillSlave $CurSlave_1_1
	}

        test "Instance #$NewMaster is now a master" {
            assert {[RI $NewMaster role] eq {master}}
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
        
        test "Instance #$CurMaster and #$StillSlave are slaves of #$NewMaster" {
            set porttmp [get_instance_attrib redis $NewMaster port]
            assert {[lindex [R $CurMaster role] 2] == $porttmp}
            assert {[lindex [R $StillSlave role] 2] == $porttmp}
        }
        # set new id to master, slave 1 and 2
        set CurSlave_1_1 $CurMaster
        set CurSlave_1_2 $StillSlave
        set CurMaster $NewMaster
    } else {
        if { [string compare [lindex [R $CurSlave_1_1 config get cluster-require-full-coverage] 1] "yes"] eq 0} {
            test "Cluster should be down now if cluster-require-full-coverage set yes" {
                assert_cluster_state fail
            }
        } else {
            test "Cluster should be still up now if cluster-require-full-coverage set no" {
                assert_cluster_state ok
            }
        }

        test "Instance #$CurSlave_1_1 and #$CurSlave_1_2 are still slaves after some time (no failover)" {
            after 5000
            assert {[RI $CurSlave_1_1 role] eq {slave}}
            assert {[RI $CurSlave_1_2 role] eq {slave}}
        }

        test "Restarting the previously killed master node" {
            restart_instance redis $CurMaster
        }

        test "Instance #$CurMaster return to a master" {
            wait_for_condition 1000 50 {
                [RI $CurMaster role] eq {master}
            } else {
                fail "Old master was not returned to master"
            }
        }
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
