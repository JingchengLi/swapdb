# real config for replication-psync.tcl
proc test_psync {descr duration backlog_size backlog_ttl delay cond reconnect} {
    start_server {tags {"repl"}
    config {real.conf}} {
        start_server {config {real.conf}} {

            set master [srv -1 client]
            set master_host [srv -1 host]
            set master_port [srv -1 port]
            set slave [srv 0 client]

            $master config set repl-backlog-size $backlog_size
            $master config set repl-backlog-ttl $backlog_ttl

            set num 100000
            set clients 3
            set clist [ start_bg_complex_data_list $master_host $master_port $num $clients 10k]

            test {Slave should be able to synchronize with the master} {
                $slave slaveof $master_host $master_port
                # redis 5 s
                wait_for_condition 200 100 {
                    [lindex [r role] 0] eq {slave} &&
                    [lindex [r role] 3] eq {connected}
                } else {
                    fail "Replication not started."
                }
            }

            test "Test replication partial resync: $descr (reconnect: $reconnect)" {
                # Now while the clients are writing data and reach transfer limit, break the maste-slave
                # link multiple times.
                wait_for_transfer_limit 1 -1
                after 3000
                if ($reconnect) {
                    for {set j 0} {$j < $duration*10} {incr j} {
                        after 100

                        if {($j % 20) == 0} {
                            catch {
                                if {$delay} {
                                    # $slave multi
                                    $slave client kill $master_host:$master_port
                                    $slave debug sleep $delay
                                    # $slave exec
                                } else {
                                    $slave client kill $master_host:$master_port
                                }
                            }
                        }
                    }
                }
                stop_bg_client_list  $clist
                wait_memory_stable -1; wait_memory_stable


                wait_for_online $master 1
                wait_for_condition 50 100 {
                    [$master debug digest] == [$slave debug digest]
                } else {
                    fail "Different digest between master([$master debug digest]) and slave([$slave debug digest]) after too long time."
                }
                assert {[$master dbsize] > 0}

                eval $cond
            }
        }
    }
}

# descr duration backlog_size backlog_ttl delay cond reconnect
test_psync {no reconnection, just sync} 6 1000000 3600 0 {
} 0

# incr 1M to 50M
test_psync {ok psync} 6 50000000 3600 0 {
    assert {[s -1 sync_partial_ok] > 0}
} 1

test_psync {no backlog} 6 100 3600 0.5 {
    assert {[s -1 sync_partial_err] > 0}
} 1

test_psync {ok after delay} 3 100000000 3600 2 {
    assert {[s -1 sync_partial_ok] > 0}
} 1

test_psync {backlog expired} 3 100000000 1 2 {
    assert {[s -1 sync_partial_err] > 0}
} 1
