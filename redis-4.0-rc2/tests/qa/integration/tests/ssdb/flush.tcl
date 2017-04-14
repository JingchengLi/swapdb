# only flush exec on master
start_server {tags {"ssdb"}} {
    set master [srv client]
    set master_port [srv port]
    set master_port [srv port]
    start_server {} {
        set slave [srv client]
        foreach flush {flushdb flushall} {
            test "$flush command" {
                r $flush
            } {OK}

            test "$flush key in ssdb and redis" {
                r set foo bar
                r set fooxxx barxxx
                dumpto_ssdb_and_wait r fooxxx
    
                r $flush
                list [r get foo] [sr get fooxxx] [r get fooxxx]
            } {{} {} {}}

            test "single client $flush all keys" {
                $master debug populate 1000000
                after 100
                $master $flush
                assert_equal "0000000000000000000000000000000000000000" [$master debug digest]
                sr -1 keys *
            } {}

            test "multi clients $flush all keys" {
                $master debug populate 1000000
                set clist [start_bg_command_list $master_host $master_port 100 $flush]
                after 1000
                stop_bg_client_list $clist

                assert_equal "0000000000000000000000000000000000000000" [$master debug digest]
                sr -1 key *
            } {}

            test "$flush and then replicate" {
                $master debug populate 1000000
                $slave debug populate 1000000
                $master $flush
                $slave slaveof $master_host $master_port
                assert_equal "0000000000000000000000000000000000000000" [$master debug digest] "master null db after $flush"
                assert_equal "0000000000000000000000000000000000000000" [$slave debug digest] "slave null db after $flush"
                list [sr key *] [sr -1 key *]
            } {{} {}}

            test "replicate and then $flush" {
                $master debug populate 1000000
                $slave debug populate 1000000
                $slave slaveof $master_host $master_port
                set pattern "Sending rr_make_snapshot to SSDB"
                wait_log_pattern $pattern [srv stdout]
                $master $flush
                assert_equal "0000000000000000000000000000000000000000" [$master debug digest] "master null db after $flush"
                assert_equal "0000000000000000000000000000000000000000" [$slave debug digest] "slave null db after $flush"
                list [sr key *] [sr -1 key *]
            } {{} {}}
        }
    }
}

start_server {tags {"ssdb"}} {
    set master [srv client]
    set master_port [srv port]
    set master_port [srv port]
    start_server {} {
        set slave [srv client]
        foreach flush {flushdb flushall} {

            test "multi clients $flush all keys during writing" {
                set num 100000
                set clients 10
                set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                set flushclist [start_bg_command_list $master_host $master_port 100 $flush]
                after 1000
                stop_bg_client_list $flushclist
            }

            test "clients can write keys after $flush" {
                wait_for_condition 100 100 {
                    [sr dbsize] > 0
                } else {
                    fail "No keys store to ssdb after $flush"
                }
                stop_bg_client_list $clist
            }

            test "master and slave are identical" {
                wait_for_condition 300 100 {
                    [$master dbsize] == [$slave dbsize]
                } else {
                    fail "Different number of keys between master and slave after too long time."
                }
                assert_equal [$slave debug digest] [$master debug digest]
            }

            test "master and slave are clear after $flush again" {
                $master $flush
                wait_for_condition 300 100 {
                    "0000000000000000000000000000000000000000" eq [$slave debug digest] &&
                    [$slave debug digest] eq [$master debug digest]
                } else {
                    fail "Different number of keys between master and slave after too long time."
                }
            }
        }
    }
}
