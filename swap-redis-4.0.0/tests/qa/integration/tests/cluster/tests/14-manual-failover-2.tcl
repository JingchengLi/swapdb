# Check the manual failover just trigger a partial sync even

source "../tests/includes/init-tests.tcl"

test "Create a 5 nodes cluster" {
    create_cluster 5 10
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Cluster is writable" {
    cluster_write_test 0
}

set current_epoch [CI 1 cluster_current_epoch]

test "Set repl-backlog-ttl to 5 in all instances" {
    foreach_redis_id id {
        R $id config set repl-backlog-ttl 10
    }
}

test "Master #0 should have two replicas attached" {
    wait_for_condition 1000 50 {
        [llength [lindex [R 0 role] 2]] == 2
    } else {
        fail "Master #0 does not have 2 slaves as expected"
    }
}

test "Full sync count at startup" {
    set startsum 0
    incr startsum [RI 0 sync_full_ok]
    incr startsum [RI 5 sync_full_ok]
    incr startsum [RI 10 sync_full_ok]
}
after 15000

for {set var 0} {$var < 2} {incr var} {
    set mid [expr ($var*5)%10]
    set sid [expr ($var*5+5)%10]
    test "Send CLUSTER FAILOVER to instance #$sid" {
        R $sid cluster failover
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

    test "Instance #$sid is now a master" {
        assert {[RI $sid role] eq {master}}
    }

    test "Master #$sid should have two replicas attached" {
        wait_for_condition 1000 50 {
            [llength [lindex [R $sid role] 2]] == 2
        } else {
            fail "Master #$sid does not have 2 slaves as expected"
        }
    }
    after 15000
}

test "Manual failover just trigger a partial sync" {
    set oksum 0
    incr oksum [RI 0 sync_full_ok]
    incr oksum [RI 5 sync_full_ok]
    incr oksum [RI 10 sync_full_ok]
    assert_equal $startsum $oksum
}
