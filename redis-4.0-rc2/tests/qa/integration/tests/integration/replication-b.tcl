# real config during writing master
# 1. connect two slaves
# 2. connect one slave and then another slave.
start_server {tags {"repl"}
config {real.conf}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set slaves {}

    start_server {config {real.conf}} {
        lappend slaves [srv 0 client]
        start_server {config {real.conf}} {
            lappend slaves [srv 0 client]

            set num 30000
            set clients 10
            set clist [ start_bg_complex_data_list $master_host $master_port $num $clients 100k]
            wait_for_transfer_limit 1 -2
            after 1000
            test "Two slaves slaveof at the same time during writing" {
                [lindex $slaves 0] slaveof $master_host $master_port
                [lindex $slaves 1] slaveof $master_host $master_port
                wait_for_online $master 2
            }
            stop_bg_client_list  $clist

            test "MASTER and SLAVES dataset should be identical after complex ops" {
                wait_memory_stable -2; wait_memory_stable -1; wait_memory_stable
                # $master config set maxmemory 0
                wait_for_condition 100 100 {
                    [$master dbsize] == [[lindex $slaves 0] dbsize] &&
                    [[lindex $slaves 0] dbsize] == [[lindex $slaves 1] dbsize]
                } else {

                    fail "Different number of keys between master and slave after too long time."
                }
                wait_for_condition 100 100 {
                    [$master debug digest] == [[lindex $slaves 0] debug digest] &&
                    [[lindex $slaves 0] debug digest] == [[lindex $slaves 1] debug digest]
                } else {
                    fail "Different digest between master([$master debug digest]) and slave1([[lindex $slaves 0] debug digest]) slave2([[lindex $slaves 1] debug digest]) after too long time."
                }
                # assert_equal [$master debug digest] [[lindex $slaves 0] debug digest] "Different digest between master and slave"
                # assert_equal [[lindex $slaves 0] debug digest] [[lindex $slaves 1] debug digest] "Different digest between slaves"
                # $master config set maxmemory 1000MB
            }
        }
    }
}

start_server {tags {"repl"}
config {real.conf}} {
    start_server {config {real.conf}} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slaves {}
        lappend slaves [srv 0 client]

        set num 30000
        set clients 10
        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients 100k]

        wait_for_transfer_limit 1 -1
        after 1000
        test "First server should have role slave after SLAVEOF" {
            [lindex $slaves 0] slaveof $master_host $master_port
            after 1000
            s 0 role
        } {slave}

        test "Slave should correctly synchronized" {
            wait_for_online $master 1
        }

        stop_bg_client_list $clist

        test "MASTER and one SLAVE dataset should be identical after complex ops" {
            wait_memory_stable -1; wait_memory_stable ; #wait slave sync done
            # $master config set maxmemory 0

            wait_for_condition 10 100 {
                [$master dbsize] == [[lindex $slaves 0] dbsize]
            } else {
                puts [ populate_diff_keys $master [lindex $slaves 0] 5 ]
                fail "Different number of keys between master and slave after too long time."
            }
            wait_for_condition 100 100 {
                [$master debug digest] == [[lindex $slaves 0] debug digest]
            } else {
                fail "Different digest between master([$master debug digest]) and slave1([[lindex $slaves 0] debug digest]) after too long time."
            }
            # $master config set maxmemory 1000MB
        }

        start_server {config {real.conf}} {
            lappend slaves [srv 0 client]

            set clist [ start_bg_complex_data_list $master_host $master_port $num $clients 10k]
            wait_for_transfer_limit 1 -2; # wait storing keys to ssdb
            test "Second server should have role slave after SLAVEOF" {
                [lindex $slaves 1] slaveof $master_host $master_port
                after 1000
                s 0 role
            } {slave}

            test "Two slaves should correctly synchronized" {
                wait_for_online $master 2
            }

            stop_bg_client_list  $clist

            test "MASTER and two SLAVES dataset should be identical after complex ops" {
                wait_memory_stable -2; wait_memory_stable -1; wait_memory_stable
                # $master config set maxmemory 0
                wait_for_condition 10 100 {
                    [$master dbsize] == [[lindex $slaves 0] dbsize] &&
                    [[lindex $slaves 0] dbsize] == [[lindex $slaves 1] dbsize]
                } else {
                    fail "Different number of keys between master and slave after too long time."
                }

                wait_for_condition 100 100 {
                    [$master debug digest] == [[lindex $slaves 0] debug digest] &&
                    [[lindex $slaves 0] debug digest] == [[lindex $slaves 1] debug digest]
                } else {
                    fail "Different digest between master([$master debug digest]) and slave1([[lindex $slaves 0] debug digest]) slave2([[lindex $slaves 1] debug digest]) after too long time."
                }
                # $master config set maxmemory 1000MB
            }
        }
    }
}
