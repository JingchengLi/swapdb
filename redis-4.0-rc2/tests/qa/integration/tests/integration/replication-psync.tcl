# Creates a master-slave pair and breaks the link continuously to force
# partial resyncs attempts, all this while flooding the master with
# write queries.
#
# You can specifiy backlog size, ttl, delay before reconnection, test duration
# in seconds, and an additional condition to verify at the end.
#
# If reconnect is > 0, the test actually try to break the connection and
# reconnect with the master, otherwise just the initial synchronization is
# checked for consistency.
proc test_psync {descr duration backlog_size backlog_ttl delay cond reconnect} {
    start_server {tags {"repl"}} {
        start_server {} {

            set master [srv -1 client]
            set master_host [srv -1 host]
            set master_port [srv -1 port]
            set slave [srv 0 client]

            $master config set repl-backlog-size $backlog_size
            $master config set repl-backlog-ttl $backlog_ttl

            set num 100000
            set clients 3
            set clist [ start_bg_complex_data_list $master_host $master_port $num $clients]

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
                # Now while the clients are writing data, break the maste-slave
                # link multiple times.
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
                wait_for_condition 200 100 {
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

test_psync {ok after delay} 3 100000000 3600 3 {
    assert {[s -1 sync_partial_ok] > 0}
} 1

test_psync {backlog expired} 3 100000000 1 3 {
    assert {[s -1 sync_partial_err] > 0}
} 1
