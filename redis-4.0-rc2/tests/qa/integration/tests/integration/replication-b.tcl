# slaveof master that owns huge data.
start_server {tags {"repl"}
config {real.conf}} {
    start_server {config {real.conf}} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave_first [srv 0 client]

        set num 500000
        set clients 5
        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients]

        wait_for_transfer_limit 1 -1

        test "First server should have role slave after SLAVEOF" {
            $slave_first slaveof $master_host $master_port
            after 1000
            s 0 role
        } {slave}

        after 1000
        stop_bg_complex_data_list $clist

        test "MASTER and SLAVE dataset should be identical after complex ops" {
            wait_memory_stable -1

            wait_for_condition 50 100 {
                [string match {*slave0:*state=online*} [$master info]]
            } else {
                fail "Slave not correctly synchronized"
            }

            wait_for_condition 10 100 {
                [$master dbsize] == [$slave_first dbsize]
            } else {
                puts [ populate_diff_keys $master $slave_first 5 ]
                fail "Different number of keys between master and slave after too long time."
            }
        }
    }
}
