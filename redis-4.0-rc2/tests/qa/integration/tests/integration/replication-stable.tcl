start_server {tags {"repl"}
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
            after 100000
            assert {[s -1 sync_full] < 2}
            # redis 5 s
            set info [r -1 info]
            stop_bg_client_list  $clist
            assert_match "*slave0:*state=online*" [r -1 info]
        }
    }
}
