# Replication migration between two machine rooms
# One Master Two Slaves

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
    create_cluster [llength $Masters] [llength $Slaves_A]
}

puts "------------masters:$Masters Slaves_A:$Slaves_A Slaves_B:$Slaves_B--------------"

test "Set datacenter-id Masters 1 Slaves_A 1 Slaves_B 2" {
    setDatacenter_id $Masters 1
    setDatacenter_id $Slaves_A 1
    setDatacenter_id $Slaves_B 2
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

    test "Instance #$CurSlave_A is slave" {
        assert {[RI $CurSlave_A role] eq {slave}}
    }

    test "Instance #$CurSlave_A synced with the master" {
        wait_for_condition 1000 50 {
            [RI $CurSlave_A master_link_status] eq {up}
        } else {
            fail "Instance #$CurSlave_A master link status is not up"
        }
    }

    set numkeys 50000
    set numops  50000
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

            # half parts replicate during load data, others after load data.
            if {( $CurMaster%2 && $j == ($numops)/2 ) || (!($CurMaster%2) && $j == $numops-1)} {
                set master_myself [get_myself $CurMaster]
                R $CurSlave_B cluster replicate [dict get $master_myself id]
            }
        }
    }

    test "Instance #$CurMaster and Instance #$CurSlave_B consistency with replication" {
        wait_for_condition 50 100 {
        [R $CurSlave_B debug digest] eq [R $CurMaster debug digest]
        } else {
            fail "Instance #$CurMaster and Instance #$CurSlave_B not consistency"
        }
    }

    set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis $CurSlave_B port]]
    test "Verify $numkeys keys for consistency with logical content" {
        # Check that the Redis Cluster content matches our logical content.
        foreach {key value} [array get content] {
            assert {[$cluster lrange $key 0 -1] eq $value}
        }
    }

    test "Instance #$CurSlave_B is slave of #$CurMaster" {
        set porttmp [get_instance_attrib redis $CurMaster port]
        assert {[lindex [R $CurSlave_B role] 2] == $porttmp}
    }    
}
