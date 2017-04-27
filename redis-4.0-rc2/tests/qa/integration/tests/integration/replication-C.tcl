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
        after 1000

        test "slave sync with master" {
            $slave slaveof $master_host $master_port
            wait_for_online $master
        }

        test "slave ssdb restart during writing" {
            after 500
            kill_ssdb_server
            after 500
            restart_ssdb_server
            set slave [srv 0 client]
            stop_bg_client_list $clist
            wait_for_condition 100 100 {
                [$master dbsize] == [$slave dbsize]
            } else {
                fail "Different number of keys between master and slave after too long time."
            }
            compare_debug_digest
        }
    }
}
