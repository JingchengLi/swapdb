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
            $client ping
        } {PONG}

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
        } {OK}
    }
}

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
            set pattern "receive.*rr_transfer_snapshot"
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
