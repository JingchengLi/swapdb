# Replica migration test.
# Check that orphaned masters are joined by replicas of masters having
# multiple replicas attached, according to the migration barrier settings.

# Set the orphaned masters datacenter-id which is different with others
set ::nodePairs 3
set pairA [list 0 $::nodePairs [expr 2*$::nodePairs ]]
set pairB [list 1 [expr 1+$::nodePairs ] [expr 1+2*$::nodePairs ]]
source "../tests/includes/init-tests.tcl"

# Create a cluster with 5 master and 10 slaves, so that we have 2
# slaves for each master.
test "Create a $::nodePairs nodes cluster" {
    create_cluster $::nodePairs [expr 2*$::nodePairs ]
}

test "Set datacenter-id pairA 1 pairB 1 Slaves_B 0" {
    setDatacenter_id $pairA 1
    setDatacenter_id $pairB 1
    # setDatacenter_id $Slaves_B 2
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Each master should have two replicas attached" {
    foreach_redis_id id {
        if {$id < $::nodePairs } {
            wait_for_condition 1000 50 {
                [llength [lindex [R $id role] 2]] == 2
            } else {
                fail "Master #$id does not have 2 slaves as expected"
            }
        }
    }
}

test "Killing all the slaves of master #0 and #1" {
    kill_instance redis [expr 0+$::nodePairs ]
    kill_instance redis [expr 0+2*$::nodePairs ]
    kill_instance redis [expr 1+$::nodePairs ]
    kill_instance redis [expr 1+2*$::nodePairs ]
    after 4000
}


if {[R 0 config get cluster-migration-barrier] < 2} {
    foreach_redis_id id {
        if {$id < $::nodePairs } {
            test "Master #$id should have at least one replica" {
                wait_for_condition 1000 50 {
                    [llength [lindex [R $id role] 2]] >= 1
                } else {
                    fail "Master #$id has no replicas"
                }
            }
        }
    }
} else {
    after [lindex [R 0 config get cluster-node-timeout] 1]
    assert {[llength [lindex [R 0 role] 2]] eq 0}
    assert {[llength [lindex [R 1 role] 2]] eq 0}
}

# Now test the migration to a master which used to be a slave, after
# a failover.

source "../tests/includes/init-tests.tcl"

test "Create a $::nodePairs nodes cluster" {
    create_cluster $::nodePairs [expr 2*$::nodePairs ]
}

test "Set datacenter-id pairA 1 pairB 1 Slaves_B 0" {
    setDatacenter_id $pairA 1
    setDatacenter_id $pairB 1
    # wait for the datacenter-id broadcast to the nodes.
    after [lindex [R 0 config get cluster-node-timeout] 1]
    # setDatacenter_id $Slaves_B 2
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Kill slave #$::nodePairs of master #0. Only slave left is #[expr 2*$::nodePairs ] now" {
    kill_instance redis $::nodePairs 
}

set current_epoch [CI 1 cluster_current_epoch]

test "Killing master node #0, #[expr 2*$::nodePairs ] should failover" {
    kill_instance redis 0
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

test "Instance [expr 2*$::nodePairs ] is now a master without slaves" {
    assert {[RI [expr 2*$::nodePairs ] role] eq {master}}
}

if {[R 1 config get cluster-migration-barrier] < 2} {
    # The remaining instance is now without slaves. Some other slave
    # should migrate to it.
    test "Master #[expr 2*$::nodePairs ] should get at least one migrated replica" {
        wait_for_condition 1000 50 {
            [llength [lindex [R [expr 2*$::nodePairs ] role] 2]] >= 1
        } else {
            fail "Master #[expr 2*$::nodePairs ] has no replicas"
        }
    } 
} else {
    after [lindex [R 1 config get cluster-node-timeout] 1]
    assert {[llength [lindex [R [expr 2*$::nodePairs ] role] 2]] eq 0}
}
