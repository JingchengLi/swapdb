# only flush exec on master
if {$::accurate} {
    set allflush {flushall flushdb}
} else {
    set allflush {flushall}
}

start_server {tags {"ssdb"}
overrides {maxmemory 0}} {
    foreach flush $allflush {
        test "set key in ssdb and redis" {
            r set foo bar
            r set fooxxx barxxx
            wait_ssdb_reconnect
            dumpto_ssdb_and_wait r fooxxx

            list [r get foo] [sr get fooxxx]
        } {bar barxxx}

        test "$flush command" {
            r $flush
        } {OK}

        test "$flush key in ssdb and redis" {
            r set foo bar
            r set fooxxx barxxx
            wait_ssdb_reconnect
            dumpto_ssdb_and_wait r fooxxx

            r $flush
            list [r get foo] [sr get fooxxx] [r get fooxxx]
        } {{} {} {}}
    }
}

# debug populate
start_server {tags {"ssdb"}} {
    set master [srv client]
    set master_host [srv host]
    set master_port [srv port]
    start_server {} {
        set slave [srv client]
        foreach flush $allflush {
            test "single client $flush all keys" {
                $master debug populate 100000
                after 500
                $master $flush
                wait_ssdb_reconnect -1

                wait_for_condition 100 100 {
                    [$master debug digest] == 0000000000000000000000000000000000000000
                } else {
                    fail "Digest not null:master([$master debug digest]) after too long time."
                }
                wait_for_condition 10 100 {
                    [lindex [sr -1 scan 0] 0] eq 0
                } else {
                    fail "ssdb not clear up:[ sr -1 scan 0]"
                }
            }

            test "multi clients $flush all keys" {
                $master debug populate 100000
                after 500
                set clist [start_bg_command_list $master_host $master_port 100 $flush]
                after 1000
                stop_bg_client_list $clist

                wait_ssdb_reconnect -1
                wait_for_condition 100 100 {
                    [$master debug digest] == 0000000000000000000000000000000000000000
                } else {
                    fail "Digest not null:master([$master debug digest]) after too long time."
                }
                wait_for_condition 10 100 {
                    [lindex [sr -1 scan 0] 0] eq 0
                } else {
                    fail "ssdb not clear up:[ sr -1 scan 0]"
                }
            }

            test "$flush and then replicate" {
                $master debug populate 100000
                $slave debug populate 100000
                after 500
                $master $flush
                $slave slaveof $master_host $master_port
                wait_for_condition 100 100 {
                    [$master debug digest] == 0000000000000000000000000000000000000000 &&
                    [$slave debug digest] == 0000000000000000000000000000000000000000
                } else {
                    fail "Digest not null:master([$master debug digest]) and slave([$slave debug digest]) after too long time."
                }
                list [lindex [sr scan 0] 0] [lindex [sr -1 scan 0] 0]
            } {0 0}

            test "replicate and then $flush" {
                $master debug populate 100000
                $slave debug populate 100000
                after 500
                $slave slaveof $master_host $master_port
                set pattern "Sending rr_make_snapshot to SSDB"
                wait_log_pattern $pattern [srv -1 stdout]
                $master $flush
                wait_for_condition 100 100 {
                    [$master debug digest] == 0000000000000000000000000000000000000000 &&
                    [$slave debug digest] == 0000000000000000000000000000000000000000
                } else {
                    fail "Digest not null:master([$master debug digest]) and slave([$slave debug digest]) after too long time."
                }
                list [lindex [sr scan 0] 0] [lindex [sr -1 scan 0] 0]
            } {0 0}

            test "replicate done and then $flush" {
                $master debug populate 100000
                $slave debug populate 100000
                after 500
                $slave slaveof $master_host $master_port
                wait_for_online $master 1
                wait_for_condition 100 100 {
                    [$master debug digest] == [$slave debug digest]
                } else {
                    fail "Different digest between master([$master debug digest]) and slave([$slave debug digest]) after too long time."
                }
                assert {[$master dbsize] == 100000}
                $master $flush
                wait_for_condition 100 100 {
                    [$master debug digest] == 0000000000000000000000000000000000000000 &&
                    [$slave debug digest] == 0000000000000000000000000000000000000000
                } else {
                    fail "Digest not null:master([$master debug digest]) and slave([$slave debug digest]) after $flush."
                }
                list [lindex [sr scan 0] 0] [lindex [sr -1 scan 0] 0]
            } {0 0}
        }
    }
}

# TODO flush IO reply error
# flush during clients writing
start_server {tags {"ssdb"}} {
    set master [srv client]
    set master_host [srv host]
    set master_port [srv port]
    start_server {} {
        set slaves {}
        lappend slaves [srv 0 client]
        start_server {} {
            lappend slaves [srv 0 client]
            set num 10000
            set clients 10
            foreach flush $allflush {
                test "single $flush all keys during clients writing after sync" {
                    set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                    [lindex $slaves 0] slaveof $master_host $master_port
                    wait_for_online $master 1
                    set size_before [$master dbsize]
                    catch { $master $flush } err
                    stop_bg_client_list $clist
                    set size_after [$master dbsize]
                    assert_equal {OK} $err "$flush should return OK"
                    assert {$size_after < $size_before}
                }

                test "master and one slave are identical after $flush" {
                    wait_for_condition 300 100 {
                        [$master dbsize] == [[lindex $slaves 0] dbsize]
                    } else {
                        fail "Different number of keys between master and slaves after too long time."
                    }
                    assert {[$master dbsize] > 0}
                    wait_for_condition 100 100 {
                        [$master debug digest] == [[lindex $slaves 0] debug digest]
                    } else {
                        fail "Different digest between master([$master debug digest]) and slave1([[lindex $slaves 0] debug digest]) after too long time."
                    }
                }

                test "multi $flush all keys during clients writing and sync" {
                    set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                    [lindex $slaves 1] slaveof $master_host $master_port
                    set pattern "Sending rr_make_snapshot to SSDB"
                    wait_log_pattern $pattern [srv -2 stdout]
                    set flushclist [start_bg_command_list $master_host $master_port 100 $flush]
                    after 1000
                    stop_bg_client_list $flushclist
                    after 1000
                    wait_ssdb_reconnect -2
                    stop_bg_client_list $clist
                    $master ping
                } {PONG}

                test "wait two slaves sync" {
                    wait_for_online $master 2
                }

                test "master and two slaves are identical after $flush" {
                    wait_for_condition 300 100 {
                        [$master dbsize] == [[lindex $slaves 0] dbsize] &&
                        [$master dbsize] == [[lindex $slaves 1] dbsize]
                    } else {
                        puts "Different number of keys between master and slaves after too long time."
                    }
                    assert {[$master dbsize] > 0}
                    wait_for_condition 100 100 {
                        [$master debug digest] == [[lindex $slaves 0] debug digest] &&
                        [$master debug digest] == [[lindex $slaves 1] debug digest]
                    } else {
                        fail "Different digest between master([$master debug digest]) and slave1([[lindex $slaves 0] debug digest]) slave2([[lindex $slaves 1] debug digest]) after too long time."
                    }
                }

            }
        }
    }
}

# flush after write
start_server {tags {"ssdb"}} {
    set master [srv client]
    set master_host [srv host]
    set master_port [srv port]
    start_server {} {
        set slaves {}
        lappend slaves [srv 0 client]
        start_server {} {
            lappend slaves [srv 0 client]
            foreach flush $allflush {
                test "multi clients $flush all keys after write and sync" {
                    set num 10000
                    set clients 10
                    set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                    [lindex $slaves 0] slaveof $master_host $master_port
                    wait_for_online $master 1
                    wait_for_condition 100 100 {
                        [sr -1 dbsize] > 0
                    } else {
                        fail "No keys store to slave ssdb"
                    }
                    stop_bg_client_list $clist
                    after 1000
                    set flushclist [start_bg_command_list $master_host $master_port 10 $flush]
                    after 1000
                    stop_bg_client_list $flushclist
                    $master ping
                } {PONG}

                test "master and slave are both null after $flush" {
                    wait_for_condition 300 100 {
                        [$master dbsize] == 0 &&
                        [[lindex $slaves 0] dbsize] == 0
                    } else {
                        fail "Different number of keys between master and slaves after too long time."
                    }
                    wait_for_condition 100 100 {
                        [$master debug digest] == [[lindex $slaves 0] debug digest] &&
                        [$master debug digest] == [[lindex $slaves 1] debug digest]
                    } else {
                        fail "Different digest between master([$master debug digest]) and slave1([[lindex $slaves 0] debug digest]) slave2([[lindex $slaves 1] debug digest]) after too long time."
                    }
                }

                test "sync with second slave after $flush" {
                    set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                    after 1000
                    [lindex $slaves 1] slaveof $master_host $master_port
                    wait_for_online $master 2
                }

                test "clients write keys after $flush" {
                    wait_for_condition 100 100 {
                        [sr -2 dbsize] > 0 &&
                        [sr -1 dbsize] > 0 &&
                        [sr dbsize] > 0
                    } else {
                        fail "No keys store to ssdb after $flush"
                    }
                    stop_bg_client_list $clist
                }

                test "master and slaves are identical" {
                    wait_for_condition 300 100 {
                        [$master dbsize] == [[lindex $slaves 0] dbsize] &&
                        [$master dbsize] == [[lindex $slaves 1] dbsize]
                    } else {
                        fail "Different number of keys between master and slaves after too long time."
                    }
                    assert {[$master dbsize] > 0}
                    wait_for_condition 100 100 {
                        [$master debug digest] == [[lindex $slaves 0] debug digest] &&
                        [$master debug digest] == [[lindex $slaves 1] debug digest]
                    } else {
                        fail "Different digest between master([$master debug digest]) and slave1([[lindex $slaves 0] debug digest]) slave2([[lindex $slaves 1] debug digest]) after too long time."
                    }
                }

                test "master and slaves are clear after $flush again" {
                    $master $flush
                    wait_for_condition 100 100 {
                        [$master debug digest] == 0000000000000000000000000000000000000000 &&
                        [[lindex $slaves 0] debug digest] == 0000000000000000000000000000000000000000 &&
                        [[lindex $slaves 1] debug digest] == 0000000000000000000000000000000000000000
                    } else {
                        fail "Digest not null:master([$master debug digest]) and slave1([[lindex $slaves 0] debug digest]) slave2([[lindex $slaves 1] debug digest]) after too long time."
                    }
                    list [lindex [sr scan 0] 0] [lindex [sr -1 scan 0] 0] [lindex [sr -2 scan 0] 0]
                } {0 0 0}

            }
        }
    }
}
