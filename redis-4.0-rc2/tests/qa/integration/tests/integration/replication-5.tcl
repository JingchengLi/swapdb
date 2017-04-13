# during master [dis]connecting slave, disconnect clients and connect clients.
start_server {tags {"repl"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set slaves {}
    start_server {} {
        lappend slaves [srv 0 client]
        start_server {} {
            lappend slaves [srv 0 client]

            set num 100000
            set clients 100
            set clist {}

            test "stress test loop slaveof and connect/disconnect all clients each time" {
                for {set n 0} {$n < 10} {incr n} {
                    if {$n % 2 == 0} {
                        [lindex $slaves 0] slaveof NO ONE
                        [lindex $slaves 1] slaveof $master_host $master_port
                        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                    } else {
                        [lindex $slaves 1] slaveof NO ONE
                        [lindex $slaves 0] slaveof $master_host $master_port
                        stop_bg_client_list  $clist
                    }
                    after 1000
                }
                list [$master ping] [[lindex $slaves 0] ping] [[lindex $slaves 1] ping]
            } {PONG PONG PONG}

            test "stress test loop slaveof and connect/disconnect one client each time" {
                if {[llength $clist] > 0} {
                    stop_bg_client_list  $clist
                }

                set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                for {set n 0} {$n < $clients} {incr n} {
                    set clist [ stop_index_bg_complex_data_in_list $clist 0 ]
                    [lindex $slaves 0] slaveof NO ONE
                    after 100
                    set clist [lappend clist [ start_bg_complex_data $master_host $master_port 0 $num ]]
                    [lindex $slaves 1] slaveof NO ONE
                    after 100
                    set clist [ stop_index_bg_complex_data_in_list $clist 0 ]
                    [lindex $slaves 0] slaveof $master_host $master_port
                    after 100
                    set clist [lappend clist [ start_bg_complex_data $master_host $master_port 0 $num ]]
                    [lindex $slaves 1] slaveof $master_host $master_port
                    after 100
                }
                stop_bg_client_list $clist
                list [$master ping] [[lindex $slaves 0] ping] [[lindex $slaves 1] ping]
            } {PONG PONG PONG}
        }
    }
}
