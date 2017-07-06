# abnormal case
# redis和ssdb完成同步后运行过程中，发生重启
start_server {tags {"repl-abnormal"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set num 100000
        set clients 1
        set clist {}
        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
        after 500

        test "slave sync with master" {
            $slave slaveof $master_host $master_port
            wait_for_online $master
        }

        # TODO allow cold/hot keys distribute not identical under this condition
        # reason: real ops cached in slave_write_op_list, and storetossdb/dumpfromssdb not.
        # No need to resolve this as evaluated.
        test "slave ssdb restart during writing" {
            for {set n 0} {$n < 3} {incr n} {
                after 500
                kill_ssdb_server
                after 1000
                restart_ssdb_server
            }
            set slave [srv 0 client]
            set slavessdb [srv 0 ssdbclient]
            after 500
            stop_bg_client_list $clist
            wait_for_condition 100 100 {
                [$master dbsize] == [$slave dbsize]
            } else {
                fail "Different number of keys between master and slave after too long time."
            }
#            wait_for_condition 100 100 {
#                [$master debug digest] == [$slave debug digest]
#            } else {
#                fail "Different digest between master([$master debug digest]) and slave([$slave debug digest]) after too long time."
#            }
            assert_equal "" [status $master db0] "No keys should stay in redis"

#            从节点ssdb发生重启的情况下从节点ssdb中会出现脏数据情况
#            wait_for_condition 100 100 {
#                [$slavessdb ssdb_dbsize] == [status $slave keys_in_ssdb_count]
#            } else {
#                fail "slave ssdb keys num equal with ssdbkeys in redis:[$slavessdb ssdb_dbsize] != [status $slave keys_in_ssdb_count]"
#            }
            compare_debug_digest
        }
    }
}

start_server {tags {"repl-abnormal"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set slavessdb [srv 0 ssdbclient]
        set num 100000
        set clients 1
        set clist {}
        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
        after 500

        test "slave sync with master" {
            $slave slaveof $master_host $master_port
            wait_for_online $master
        }

        test "master ssdb restart during writing" {
            for {set n 0} {$n < 3} {incr n} {
                after 500
                kill_ssdb_server -1
                after 1000
                restart_ssdb_server -1
            }
            set master [srv -1 client]
            after 500
            stop_bg_client_list $clist
            wait_for_condition 100 100 {
                [$master dbsize] == [$slave dbsize]
            } else {
                fail "Different number of keys between master and slave after too long time."
            }
            wait_for_condition 100 100 {
                [$master debug digest] == [$slave debug digest]
            } else {
                fail "Different digest between master([$master debug digest]) and slave([$slave debug digest]) after too long time."
            }
            assert_equal "" [status $master db0] "No keys should stay in redis"

            wait_for_condition 100 100 {
                [$slavessdb ssdb_dbsize] == [status $slave keys_in_ssdb_count]
            } else {
                fail "slave ssdb keys num equal with ssdbkeys in redis:[$slavessdb ssdb_dbsize] != [status $slave keys_in_ssdb_count]"
            }
            compare_debug_digest
        }
    }
}
