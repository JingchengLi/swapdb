# Slave selection test
# Check the algorithm trying to pick the slave with the most complete history.

# set nodePairs 5
set Masters {}
set Slaves_A {}
set Slaves_B {}
for {set n 0} {$n < $::nodePairs } {incr n} {
    lappend Masters $n
    lappend Slaves_A [expr $::nodePairs +$n]
    lappend Slaves_B [expr 2*$::nodePairs +$n]
    lappend Slaves_C [expr 3*$::nodePairs +$n]
}

source "../tests/includes/init-tests.tcl"

test "Create a $::nodePairs nodes cluster" {
    create_cluster [llength $Masters] [expr [llength $Slaves_A]+[llength $Slaves_B]+[llength $Slaves_C]]
}

puts "------------masters:$Masters Slaves_A:$Slaves_A Slaves_B:$Slaves_B Slaves_C:$Slaves_C--------------"
test "Set datacenter-id Masters 1 Slaves_A 1 Slaves_B 1 Slaves_C 2" {
    setDatacenter_id $Masters 1
    setDatacenter_id $Slaves_A 1
    setDatacenter_id $Slaves_B 1
    setDatacenter_id $Slaves_C 2
    # wait for the datacenter-id broadcast to the nodes.
    after [lindex [R 0 config get cluster-node-timeout] 1]
}

test "Cluster is up" {
    assert_cluster_state ok
}

foreach CurMaster $Masters CurSlave_A $Slaves_A CurSlave_B $Slaves_B CurSlave_C $Slaves_C {

    test "The master #$CurMaster has actually three slaves" {
        assert {[llength [lindex [R $CurMaster role] 2]] == 3}
    }

    test "Slaves of #$CurMaster are instance #$CurSlave_A #$CurSlave_B and #$CurSlave_C expected" {
        set portM [get_instance_attrib redis $CurMaster port]
        assert {[lindex [R $CurSlave_A role] 2] == $portM}
        assert {[lindex [R $CurSlave_B role] 2] == $portM}
        assert {[lindex [R $CurSlave_C role] 2] == $portM}
    }

    test "Instance #$CurSlave_A #$CurSlave_B and #$CurSlave_C synced with the master" {
        wait_for_condition 1000 50 {
            [RI $CurSlave_A master_link_status] eq {up} &&
            [RI $CurSlave_B master_link_status] eq {up} &&
            [RI $CurSlave_C master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave_A #$CurSlave_B or #$CurSlave_C master link status is not up"
        }
    }

    set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis $CurMaster port]]

    test "Slaves are all able to receive and acknowledge writes" {
        for {set j 0} {$j < 100} {incr j} {
            $cluster set $j $j
        }
        assert {[R $CurMaster wait 2 60000] == 3}
    }

    test "Write data while slave #$CurSlave_B is paused and can't receive it" {
        # Stop the slave with a multi/exec transaction so that the master will
        # be killed as soon as it can accept writes again.
        # R $CurSlave_B multi
        R $CurSlave_B deferred 1
        R $CurSlave_B client kill 127.0.0.1:$portM
        R $CurSlave_B debug sleep 10
        # R $CurSlave_B exec

        # Write some data the slave can't receive.
        for {set j 0} {$j < 100} {incr j} {
            $cluster set $j $j
        }

        # Prevent the master from accepting new slaves.
        # Use a large pause value since we'll kill it anyway.
        R $CurMaster CLIENT PAUSE 60000

        # Wait for the slave to return available again
        R $CurSlave_B deferred 0
        assert {[R $CurSlave_B read] eq {OK OK}}

        # Kill the master so that a reconnection will not be possible.
        kill_instance redis $CurMaster
    }

    test "Wait for instance #$CurSlave_A (and not #$CurSlave_B or $CurSlave_C) to turn into a master" {
        wait_for_condition 1000 50 {
            [RI $CurSlave_A role] eq {master}
        } else {
            fail "No failover detected"
        }
    }

    test "Wait for the node #$CurSlave_B to return alive before ending the test" {
        R $CurSlave_B ping
    }

    test "Cluster should eventually be up again" {
        assert_cluster_state ok
    }

    test "Node #$CurSlave_B and #$CurSlave_C should eventually replicate node #$CurSlave_A" {
        set portA [get_instance_attrib redis $CurSlave_A port]
        wait_for_condition 1000 50 {
            ([lindex [R $CurSlave_B role] 2] == $portA) &&
            ([lindex [R $CurSlave_C role] 2] == $portA) &&
            ([lindex [R $CurSlave_B role] 3] eq {connected}) &&
            ([lindex [R $CurSlave_C role] 3] eq {connected})
        } else {
            fail "#$CurSlave_B and #$CurSlave_C didn't become slave of #$CurSlave_A"
        }
    }
}
