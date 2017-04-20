start_server {tags {"repl-stable"}
config {real.conf}} {
    start_server {config {real.conf}} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]

        set num 1000000
        set clients 3
        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients 10k]

        test {Slave should be able to synchronize with the master} {
            $slave slaveof $master_host $master_port
            after 500000
            stop_bg_client_list  $clist
            assert_match "*slave0:*state=online*" [r -1 info]
        }

        test "master and slave are identical after long time" {
            wait_for_condition 300 100 {
                [$master dbsize] == [$slave dbsize]
            } else {
                fail "Different number of keys between master and slaves after too long time."
            }

            assert {[$master dbsize] > 0}
            wait_for_condition 100 100 {
                [$master debug digest] == [$slave debug digest]
            } else {
                fail "Different digest between master([$master debug digest]) and slave([$slaves debug digest]) after too long time."
            }
        }

        test {Expect only full sync once} {
            assert {[s -1 sync_full] < 2}
        }
    }
}
