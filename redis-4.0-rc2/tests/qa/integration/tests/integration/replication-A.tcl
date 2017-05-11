# abnormal case
# 当ssdb挂掉时，redis需要进行重连。包括客户端、ssdb_client和ssdb_replication_client等。
# new client is client connected after kill and restart ssdb
# mid client is client connected between kill and restart ssdb
# old client is client connected before kill and restart ssdb
foreach flag {New Mid Old} {
    start_server {tags {"repl-abnormal"}} {
        set client [redis [srv host] [srv port]]
        test "Before restart ssdb set key store to ssdb ($flag client)" {
            $client set foo bar
            wait_for_dumpto_ssdb r foo
        }

        test "Should reconnect when ssdb restart ($flag client)" {
            kill_ssdb_server
            if {"Mid" == $flag} {
                set client [redis [srv host] [srv port]]
            }
            restart_ssdb_server
            if {"New" == $flag} {
                set client [redis [srv host] [srv port]]
            }
            after 3000
            if {"New" == $flag} {
                assert_equal {PONG} [$client ping] "New client should connect"
            } else {
                catch {$client ping} err
                assert_equal {PONG} $err "Currently cannot support $flag client reconnect."
            }
        }

        if {"New" == $flag || "Mid" == $flag || "Old" == $flag} {
            test "After restart ssdb access cold key ($flag client)" {
                $client get foo
            } {bar}

            test "After restart ssdb set new key ($flag client)" {
                $client set hello world
            } {OK}

            test "After restart ssdb access new key ($flag client)" {
                $client get hello
            } {world}

            test "After restart ssdb del cold key ($flag client)" {
                $client del foo
            } {1}
        }
    }
}

# swap-34
# 从节点ssdb在开始传输快照时重启，验证同步完成后key基本读写情况。
start_server {tags {"repl-abnormal"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        $master set mykey foo
        set slave [srv 0 client]

        test "slave ssdb restart when master start to transfer snapshot" {
            $slave slaveof $master_host $master_port
            set pattern "transfer_snapshot start"
            wait_log_pattern $pattern [srv -1 ssdbstdout]

            kill_ssdb_server
            restart_ssdb_server

            set slave [srv 0 client]
            wait_for_condition 50 500 {
                [lindex [$slave role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$slave info replication]]
            } else {
                fail "slave can not sync with master during writing"
            }
            wait_for_online $master
        }

        test {Sync should have transferred keys from master} {
            $slave get mykey
        } {foo}

        test {SET on the master should immediately propagate} {
            $master set mykey2 bar
            wait_for_condition 10 100 {
                [$slave get mykey2] eq {bar}
            } else {
                fail "SET on master did not propagated on slave"
            }
        }

        after 100

        test {slave should ping-pong after sometime} {
            $slave ping
        } {PONG}
    }
}

# swap-45
## 如果在快照删除的超时时间内redis又发了rr_make_snapshot，ssdb也要释放前一次的快照并重新生成，即保证ssdb中只存在一份快照。
start_server {tags {"repl-abnormal"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master set mykey foo
    set slaves {}
    start_server {} {
        lappend slaves [srv 0 client]
        start_server {} {
            lappend slaves [srv 0 client]

            test "all slaves online after make snapshot again during deleting snapshot" {
                [lindex $slaves 0] slaveof $master_host $master_port
                set pattern "snapshot transfer finished"
                wait_log_pattern $pattern [srv -2 stdout]
                [lindex $slaves 1] slaveof $master_host $master_port
                wait_for_online $master 2
            }

            test "Sync should have transferred keys from master" {
                assert_equal {foo} [[lindex $slaves 0] get mykey]
                assert_equal {foo} [[lindex $slaves 1] get mykey]
            }

           test "SET on the master should immediately propagate" {
               $master set mykey2 bar
               wait_for_condition 10 100 {
                   [[lindex $slaves 0] get mykey2] eq {bar} &&
                   [[lindex $slaves 1] get mykey2] eq {bar}
               } else {
                   fail "SET on master did not propagated on slaves"
               }
            }

           after 100

           test {slave should ping-pong after sometime} {
                assert_equal {PONG} [[lindex $slaves 0] ping]
                assert_equal {PONG} [[lindex $slaves 1] ping]
            }
        }
    }
}
