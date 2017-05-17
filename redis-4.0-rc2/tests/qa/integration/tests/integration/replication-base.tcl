start_server {tags {"repl"}} {
    set A [srv 0 client]
    set A_host [srv 0 host]
    set A_port [srv 0 port]
    start_server {} {
        set B [srv 0 client]
        set B_host [srv 0 host]
        set B_port [srv 0 port]

        test {Set instance A as slave of B} {
            $A slaveof $B_host $B_port
            wait_for_condition 50 100 {
                [lindex [$A role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$A info replication]]
            } else {
                fail "Can't turn the instance into a slave"
            }
        }
    }
}

start_server {tags {"repl"}} {
    set A [srv 0 client]
    set A_host [srv 0 host]
    set A_port [srv 0 port]
    $A debug populate 1000
    $A set foo bar
    start_server {} {
        set B [srv 0 client]
        set B_host [srv 0 host]
        set B_port [srv 0 port]
        $B debug populate 1000
        $B set fooB barB
        after 500

        test {A with keys slaveof B} {
            $A slaveof $B_host $B_port
            wait_for_condition 50 100 {
                [lindex [$A role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$A info replication]]
            } else {
                fail "Can't turn the instance into a slave"
            }
        }

        test {Slave A is identical with Master B} {
            wait_for_condition 10 500 {
                [$A debug digest] == [$B debug digest]
            } else {
                fail "Digest not equal:master([$A debug digest]) and slave([$B debug digest]) after too long time."
            }
            assert_equal {barB} [$A get fooB]
            assert_equal {} [$A get foo]
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

        test {slave can only be loaded by master} {
            r config set maxmemory 0
            r -1 config set maxmemory 0

            assert_equal [r -1 get mykey] {foo}
            assert_equal [r 0 get mykey2] {bar}
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

        test {slave should dump key to ssdb when conditions satisfied} {
            set oldlower [lindex [ r -1 config get ssdb-transfer-lower-limit ] 1]
            r -1 config set ssdb-transfer-lower-limit 0
            set oldmaxmemory [lindex [ r -1 config get maxmemory ] 1]
            r -1 config set maxmemory 100M
            r -1 set foossdb 12345
            wait_for_condition 50 100 {
                [r -1 locatekey foossdb] eq {ssdb}
            } else {
                fail "master's key should be dump to ssdb"
            }
            wait_for_condition 50 100 {
                [r locatekey foossdb] eq {ssdb}
            } else {
                fail "slave's key should be dump to ssdb"
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

# master with only one slave
foreach dl {no} {
    start_server {tags {"repl"}} {
        set master [srv 0 client]
        $master config set repl-diskless-sync $dl
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set slaves {}
        set load_handle0 [start_write_load $master_host $master_port 3 10]
        set load_handle1 [start_write_load $master_host $master_port 5 10]
        set load_handle2 [start_write_load $master_host $master_port 20 10]
        set load_handle3 [start_write_load $master_host $master_port 8 10]
        set load_handle4 [start_write_load $master_host $master_port 4 10]
        start_server {} {
            lappend slaves [srv 0 client]
            test "Connect single slave at the same time , diskless=$dl" {
                # Send SLAVEOF commands to slave
                [lindex $slaves 0] slaveof $master_host $master_port

                # Wait for the slave to reach the "online"
                # state from the POV of the master.
                set retry 500
                while {$retry} {
                    set info [$master info]
                    if {[string match {*slave0:*state=online*} $info]} {
                        break
                    } else {
                        incr retry -1
                        after 100
                    }
                }
                if {$retry == 0} {
                    error "assertion:Single Slave not correctly synchronized"
                }

                # Wait that slave acknowledge is online so
                # we are sure that DBSIZE and DEBUG DIGEST will not
                # fail because of timing issues.
                wait_for_condition 500 100 {
                    [lindex [[lindex $slaves 0] role] 3] eq {connected}
                } else {
                    fail "Single Slave still not connected after some time"
                }

                # Stop the write load
                stop_write_load $load_handle0
                stop_write_load $load_handle1
                stop_write_load $load_handle2
                stop_write_load $load_handle3
                stop_write_load $load_handle4

                wait_for_condition 500 100 {
                    [$master debug digest] == [[lindex $slaves 0] debug digest]
                } else {
                    fail "Different digest between master and single slave after too long time."
                }

                assert {[$master debug digest] ne 0000000000000000000000000000000000000000}

                # Check digests when all keys be hot
                r -1 config set maxmemory 0
                set digest [debug_digest r -1]
                set digest2 [debug_digest r 0]
                wait_for_condition 500 100 {
                    $digest == [r debug digest]
                } else {
                    fail "Different digest between master and single slave after too long time."
                }
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
        set load_handle0 [start_write_load $master_host $master_port 3 10]
        set load_handle1 [start_write_load $master_host $master_port 5 10]
        set load_handle2 [start_write_load $master_host $master_port 20 10]
        set load_handle3 [start_write_load $master_host $master_port 8 10]
        set load_handle4 [start_write_load $master_host $master_port 4 10]
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
                        stop_write_load $load_handle3
                        stop_write_load $load_handle4

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

                        # Check digests when all keys be hot
                        r -3 config set maxmemory 0
                        set digest [debug_digest r -3]
                        set digest0 [debug_digest r -2]
                        set digest1 [debug_digest r -1]
                        set digest2 [debug_digest r 0]
                        wait_for_condition 500 100 {
                            $digest == [[lindex $slaves 0] debug digest] &&
                            $digest == [[lindex $slaves 1] debug digest] &&
                            $digest == [[lindex $slaves 2] debug digest]
                        } else {
                            fail "Different digest between master and slave after too long time."
                        }
                    }
               }
            }
        }
    }
}
