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

