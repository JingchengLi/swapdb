start_server {tags {"ssdb"}
overrides {maxmemory 0}} {
    test "ssdbinfo basic" {
        r set foo bar
        r set fooxxx barxxx
        dumpto_ssdb_and_wait r fooxxx
        assert_equal 1 [s "keys in redis count"] 
        assert_equal 1 [s "keys in SSDB count"] 
        wait_keys_processed r
    }
}

start_server {tags {"repl"}} {
    set master [srv client]
    start_server {} {
        test "First server should have role slave after SLAVEOF" {
            r 0 slaveof [srv -1 host] [srv -1 port]
            wait_for_online $master
        }

        test "write after online wait ssdbinfo show keys transfer/load/visit done" {
            createComplexDataset $master 1000
            wait_keys_processed r {0 -1}
        }
    }
}

start_server {tags {"ssdb"}} {
    set master [srv client]
    set master_host [srv host]
    set master_port [srv port]
    start_server {} {
        set slaves {}
        lappend slaves [srv 0 client]
        start_server {} {
            lappend slaves [srv 0 client]
            test "ssdbinfo show keys transfer/load/visit during write" {
                set num 10000
                set clients 10
                set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                [lindex $slaves 0] slaveof $master_host $master_port
                wait_for_online $master 1
                wait_for_condition 100 100 {
                   [s -2 "keys loading from SSDB"] > 0 &&
                   [s -2 "keys transferring to SSDB"] > 0 &&
                   [s -2 "keys visiting SSDB"] > 0
                } else {
                    fail "keys should transfer/load/visit ssdb in master"
                }
                wait_for_condition 100 100 {
                   [s -1 "keys loading from SSDB"] > 0 &&
                   [s -1 "keys transferring to SSDB"] > 0 &&
                   [s -1 "keys visiting SSDB"] > 0
                } else {
                    fail "keys should transfer/load/visit ssdb in slave"
                }
                stop_bg_client_list $clist
            }

            test "ssdbinfo show keys transfer/load/visit done by info clear" {
                wait_keys_processed r {-1 -2}
            }

            test "sync with second slave" {
                set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                after 1000
                [lindex $slaves 1] slaveof $master_host $master_port
                wait_for_online $master 2
                stop_bg_client_list $clist
            }

            test "master and slaves are clear after flushall" {
                wait_keys_processed r {0 -1 -2}
                $master flushall
                check_keys_cleared r {0 -1 -2}
            }
        }
    }
}

