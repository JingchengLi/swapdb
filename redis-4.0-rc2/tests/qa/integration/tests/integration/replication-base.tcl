start_server {tags {"repl"}} {
    set A [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    start_server {} {
        set B [srv 0 client]

        test {Set instance B as slave of A} {
            $B slaveof $master_host $master_port
            wait_for_condition 50 100 {
                [lindex [$B role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$B info replication]]
            } else {
                fail "Can't turn the instance into a slave"
            }
        }
    }
}

start_server {tags {"repl"}} {
    set A [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $A debug populate 1000
    $A set foo bar
    start_server {} {
        set B [srv 0 client]
        $B debug populate 1000
        $B set fooB barB
        after 500

        test {instance B with keys slaveof instance A} {
            $B slaveof $master_host $master_port
            wait_for_condition 50 100 {
                [lindex [$B role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$B info replication]]
            } else {
                fail "Can't turn the instance into a slave"
            }
            wait_for_online $A
        }

        test {Slave B is identical with Master A} {
            wait_for_condition 10 500 {
                [$A debug digest] == [$B debug digest]
            } else {
                fail "Digest not equal:master([$A debug digest]) and slave([$B debug digest]) after too long time."
            }
            assert_equal {bar} [$B get foo]
            assert_equal {} [$B get fooB]
        }
    }
}

start_server {tags {"repl"}} {
    r set mykey foo

    start_server {} {
        test {Second server should have role master at first} {
            s role
        } {master}

        test {SLAVEOF should start with link status "down"} {
            r slaveof [srv -1 host] [srv -1 port]
            s master_link_status
        } {down}

        test {The role should immediately be changed to "slave"} {
            s role
        } {slave}

        wait_for_sync r
        wait_for_online [srv -1 client]

        test {Sync should have transferred keys from master} {
            r get mykey
        } {foo}

        test {The link status should be up} {
            s master_link_status
        } {up}

        test {SET on the master should immediately propagate} {
            r -1 set mykey2 bar

            wait_for_condition 10 100 {
                [r 0 get mykey2] eq {bar}
            } else {
                fail "SET on master did not propagated on slave"
            }
        }

        test {key can only be loaded by master} {
            r config set maxmemory 0
            r -1 config set maxmemory 0

            # assert_equal [r -1 get mykey] {foo}
            # assert_equal [r 0 get mykey2] {bar}
            access_key_tps_time mykey 200 1 true -1
            access_key_tps_time mykey2 200 1 true 0
            wait_for_condition 100 10 {
                [r 0 locatekey mykey] eq {redis} &&
                [r 0 locatekey mykey2] eq {ssdb}
            } else {
                fail "slave key status error"
            }
            wait_for_condition 100 10 {
                [r -1 locatekey mykey] eq {redis} &&
                [r -1 locatekey mykey2] eq {ssdb}
            } else {
                fail "master key status error"
            }
        }

        test {verify key in ssdb match its status} {
            assert_equal [sr 0 get mykey] {} "slave key with status redis should not in ssdb"
            assert_equal [sr 0 get mykey2] {bar} "slave key with status ssdb should in ssdb"
            assert_equal [sr -1 get mykey] {} "master key with status redis should not in ssdb"
            assert_equal [sr -1 get mykey2] {bar} "master key with status ssdb should in ssdb"
        }

        test {key can only store to ssdb by master} {
            r -1 set foossdb 12345
            r config set ssdb-transfer-lower-limit 0
            r config set maxmemory 100M
            after 3000
            assert_equal {redis} [r locatekey foossdb] "key should not store to ssdb when only slave statisfy transfer condition"

            set oldlower [lindex [ r -1 config get ssdb-transfer-lower-limit ] 1]
            r -1 config set ssdb-transfer-lower-limit 0
            set oldmaxmemory [lindex [ r -1 config get maxmemory ] 1]
            r -1 config set maxmemory 100M
            wait_for_condition 50 100 {
                [r -1 locatekey foossdb] eq {ssdb}
            } else {
                fail "master's key should store to ssdb"
            }
            wait_for_condition 50 100 {
                [r locatekey foossdb] eq {ssdb}
            } else {
                fail "slave's key should store to ssdb"
            }
            r -1 config set maxmemory $oldmaxmemory
            r -1 config set ssdb-transfer-lower-limit $oldlower
        }

        test {#issue slave key's value is missed} {
            assert_equal [r -1 get foossdb] {12345} "master foossdb value error"
            assert_equal [r get foossdb] {12345} "slave foossdb value error"
        }

        test {FLUSHALL should replicate} {
            r -1 flushall
            after 500
            # wait_flushall
            if {$::valgrind} {after 2000}
            list [r -1 dbsize] [r 0 dbsize]
        } {0 0}

        test {ROLE in master reports master with a slave} {
            set res [r -1 role]
            lassign $res role offset slaves
            assert {$role eq {master}}
            assert {$offset > 0}
            assert {[llength $slaves] == 1}
            lassign [lindex $slaves 0] master_host master_port slave_offset
            assert {$slave_offset <= $offset}
        }

        test {ROLE in slave reports slave in connected state} {
            set res [r role]
            lassign $res role master_host master_port slave_state slave_offset
            assert {$role eq {slave}}
            assert {$slave_state eq {connected}}
        }
    }
}

start_server {tags {"repl"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set slaves {}
    start_server {} {
        lappend slaves [srv 0 client]
        start_server {} {
            lappend slaves [srv 0 client]
            test {Two slaves slaveof at the same time} {
                # Send SLAVEOF commands to slaves
                [lindex $slaves 0] slaveof $master_host $master_port
                [lindex $slaves 1] slaveof $master_host $master_port
                wait_for_online $master 2
            }
        }
    }
}

start_server {tags {"repl"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set slaves {}
    start_server {} {
        lappend slaves [srv 0 client]
        start_server {} {
            lappend slaves [srv 0 client]
            test {Two slaves slaveof in turn} {
                [lindex $slaves 0] slaveof $master_host $master_port
                wait_for_online $master 1
                [lindex $slaves 1] slaveof $master_host $master_port
                wait_for_online $master 2
            }
        }
    }
}

# not support diskless replication.
foreach dl {no} {
    start_server {tags {"repl"}} {
        set master [srv 0 client]
        $master config set repl-diskless-sync $dl
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set slaves {}
        set load_handle0 [start_write_load $master_host $master_port 3]
        set load_handle1 [start_write_load $master_host $master_port 5]
        set load_handle2 [start_write_load $master_host $master_port 20]
        start_server {} {
            lappend slaves [srv 0 client]
            start_server {} {
                lappend slaves [srv 0 client]
                start_server {} {
                    lappend slaves [srv 0 client]
                    test "Connect multiple slaves at the same time (issue #141), diskless=$dl" {
                        # Send SLAVEOF commands to slaves
                        [lindex $slaves 0] slaveof $master_host $master_port
                        [lindex $slaves 1] slaveof $master_host $master_port
                        [lindex $slaves 2] slaveof $master_host $master_port

                        # Wait for all the three slaves to reach the "online"
                        # state from the POV of the master.
                        puts "cost [wait_for_online $master 3]s to be online!"

                        # Wait that slaves acknowledge they are online so
                        # we are sure that DBSIZE and DEBUG DIGEST will not
                        # fail because of timing issues.
                        wait_for_condition 100 100 {
                            [lindex [[lindex $slaves 0] role] 3] eq {connected} &&
                            [lindex [[lindex $slaves 1] role] 3] eq {connected} &&
                            [lindex [[lindex $slaves 2] role] 3] eq {connected}
                        } else {
                            fail "Slaves still not connected after some time"
                        }

                        # Stop the write load
                        stop_write_load $load_handle0
                        stop_write_load $load_handle1
                        stop_write_load $load_handle2

                        # Make sure that slaves and master have same
                        # number of keys
                        wait_for_condition 300 100 {
                            [$master dbsize] == [[lindex $slaves 0] dbsize] &&
                            [$master dbsize] == [[lindex $slaves 1] dbsize] &&
                            [$master dbsize] == [[lindex $slaves 2] dbsize]
                        } else {
                            fail "Different number of keys between master and slave after too long time."
                        }

                        wait_for_condition 100 100 {
                            [$master debug digest] == [[lindex $slaves 0] debug digest] &&
                            [[lindex $slaves 0] debug digest] == [[lindex $slaves 1] debug digest] &&
                            [[lindex $slaves 1] debug digest] == [[lindex $slaves 2] debug digest]
                        } else {
                            fail "Different digest between master([$master debug digest]) and slave1([[lindex $slaves 0] debug digest]) slave2([[lindex $slaves 1] debug digest]) slave3([[lindex $slaves 2] debug digest]) after too long time."
                        }
                        assert {[$master debug digest] ne 0000000000000000000000000000000000000000}

                        set somekeys [r -3 keys 0.000*]
                        assert {[llength $somekeys] > 0} "should compare some keys"
                        foreach key $somekeys {
                            set val [r -3 get $key]
                            assert_equal $val [[lindex $slaves 0] get $key] "key:$key not identical between master and slave0!"
                            assert_equal $val [[lindex $slaves 1] get $key] "key:$key not identical between master and slave1!"
                            assert_equal $val [[lindex $slaves 2] get $key] "key:$key not identical between master and slave2!"
                        }
                    }
                }
            }
        }
    }
}
