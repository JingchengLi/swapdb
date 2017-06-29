# create this test script from psync2.tcl for [SWAP-77]
start_server {tags {"kill link"}} {
start_server {} {
    set master [srv -1 client]
    set master_host [srv -1 host]
    set master_port [srv -1 port]
    set slave [srv 0 client]
    set counter_value 0             ; # Current value of the Redis counter "x"

    set genload 1                   ; # Load master with writes at every cycle

    set genload_time 10000

    set disconnect 1                ; # Break replication link between random
                                      # master and slave instances while the
                                      # master is loaded with writes.

    set disconnect_period 1000      ; # Disconnect repl link every N ms.

    test "Slave is consistent with Master" {
        $master set x $counter_value
        $slave slaveof $master_host $master_port
        wait_for_condition 50 1000 {
            [$slave get x] == $counter_value
        } else {
            fail "$slave x variable is inconsistent"
        }
    }

    test "generate load while killing replication links" {
        set t [clock milliseconds]
        set next_break [expr {$t+$disconnect_period}]
        while {[clock milliseconds]-$t < $genload_time} {
            if {$genload} {
                $master incr x; incr counter_value
            }
            if {[clock milliseconds] == $next_break} {
                set next_break \
                    [expr {[clock milliseconds]+$disconnect_period}]
                $slave client kill type master
            }
        }
    }

    test "cluster is consistent after load (x = $counter_value)" {
        wait_for_condition 50 1000 {
            [$slave get x] == $counter_value
        } else {
            fail "slave x variable is inconsistent"
        }
    }
}}
