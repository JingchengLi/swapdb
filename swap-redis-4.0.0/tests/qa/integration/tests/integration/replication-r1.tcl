start_server {tags {"repl"}
config {real.conf}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    start_server {config {real.conf}} {
        set slave [srv 0 client]
        test "First server should have role slave after SLAVEOF" {
            r slaveof [srv -1 host] [srv -1 port]
            after 1000
            s role
        } {slave}

        test "MASTER and SLAVE dataset should be identical after complex ops and reach transfer limit" {
            $master config set maxmemory 100M
            set pre [clock seconds]
            set num 300000
            set clients 3
            set clist [ start_bg_complex_data_list $master_host $master_port $num $clients {1k useexpire}]
            wait_for_transfer_limit 1 -1
            after 5000
            stop_bg_client_list $clist
            after 4000
            r -1 config set maxmemory 0
            compare_debug_digest
            wait_for_condition 50 100 {
                [llength [sr -1 keys *] ] eq 0
            } else {
                fail "master's ssdb keys should be null"
            }
            wait_for_condition 50 100 {
                [llength [sr keys *]] eq [status $slave keys_in_ssdb_count]
            } else {
                fail "slave's ssdb keys count should be equal to count in redis!"
            }
        }
    }
}
