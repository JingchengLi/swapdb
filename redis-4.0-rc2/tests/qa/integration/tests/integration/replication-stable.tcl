start_server {tags {"repl-stable"}
config {real.conf}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set slaves {}
    start_server {config {real.conf}} {
        lappend slaves [srv 0 client]
        start_server {config {real.conf}} {
            lappend slaves [srv 0 client]

            set num 1000000
            set clients 2
            set clist [ start_bg_complex_data_list $master_host $master_port $num $clients 1k]
            lappend clist {*}[ start_bg_complex_data_list $master_host $master_port $num $clients 10k]
            lappend clist {*}[ start_bg_complex_data_list $master_host $master_port $num $clients 100k]

            test {First Slave should be able to synchronize with the master} {
                [lindex $slaves 0] slaveof $master_host $master_port
                after 500000
                stop_bg_client_list $clist
                wait_for_online $master 1 1
            }

            test "master and slave are identical after long time" {
                wait_for_condition 30 1000 {
                    [$master dbsize] == [[lindex $slaves 0] dbsize]
                } else {
                    fail "Different number of keys between master and slave after too long time."
                }

                assert {[$master dbsize] > 0}
                wait_for_condition 100 100 {
                    [$master debug digest] == [[lindex $slaves 0] debug digest]
                } else {
                    fail "Different digest between master([$master debug digest]) and slave([[lindex $slaves 0] debug digest]) after too long time."
                }
            }

            test {Expect only full sync once} {
                assert {[s -2 sync_full] == 1}
            }

            test {Second Slave should be able to synchronize with the master} {
                set clist [ start_bg_complex_data_list $master_host $master_port $num $clients 1k]
                lappend clist {*}[ start_bg_complex_data_list $master_host $master_port $num $clients 10k]
                lappend clist {*}[ start_bg_complex_data_list $master_host $master_port $num $clients 100k]
                after 5000
                [lindex $slaves 1] slaveof $master_host $master_port
                after 500000
                stop_bg_client_list $clist
                wait_for_online $master 2 1
            }

            test "master and two slaves are identical after long time" {
                wait_for_condition 30 1000 {
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
                    fail "Different digest between master([$master debug digest]) and slave1([[lindex $slaves 0] debug digest]) : slave2([[lindex $slaves 1] debug digest]) after too long time."
                }
            }

            test {Expect two full syncs for two slaves} {
                assert {[s -2 sync_full] == 2}
            }
        }
    }
}
#
#start_server {tags {"repl-stable"}
#config {real.conf}} {
#    start_server {config {real.conf}} {
#
#        set master [srv -1 client]
#        set master_host [srv -1 host]
#        set master_port [srv -1 port]
#        set slave [srv 0 client]
#
#        set num 1000000
#        set clients 3
#        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients 10k]
#
#        test {Slave should be able to synchronize with the master} {
#            $slave slaveof $master_host $master_port
#            after 500000
#            stop_bg_client_list  $clist
#            assert_match "*slave0:*state=online*" [r -1 info]
#        }
#
#        test "master and slave are identical after long time" {
#            wait_for_condition 300 100 {
#                [$master dbsize] == [$slave dbsize]
#            } else {
#                fail "Different number of keys between master and slaves after too long time."
#            }
#
#            assert {[$master dbsize] > 0}
#            wait_for_condition 100 100 {
#                [$master debug digest] == [$slave debug digest]
#            } else {
#                fail "Different digest between master([$master debug digest]) and slave([$slave debug digest]) after too long time."
#            }
#        }
#
#        test {Expect only full sync once} {
#            assert {[s -1 sync_full] < 2}
#        }
#    }
#}
