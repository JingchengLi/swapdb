# abnormal case
# 如果刚好加载/转存ssdb key时redis挂了，导致redis的db[0]和evictdb中同时存在一个key的情况。
# TODO
# redis client double free问题。
# TODO

## 在主从复制的任一阶段，将从节点ssdb搞挂，恢复后是否还能重新发起主从复制。
set all {
    slaveredis "SLAVE OF.*enabled"
    masterredis "Slave.*asks for synchronization"
    slaveredis "MASTER <-> SLAVE sync started"
    masterredis "Starting BGSAVE for SYNC with target"
    slaveredis "Full resync from master"
    masterredis "DB saved on disk"
    masterredis "Background saving terminated"
    masterssdb "transfer_snapshot start"
    slaveredis "MASTER <-> SLAVE sync: Finished with success"
    slaveredis "MASTER <-> SLAVE sync: Flushing old data"
    masterredis "Synchronization with slave.*succeeded"
}

if {$::loglevel == "debug"} {
    lappend all {*}{
        masterssdb "receive.*rr_check_write"
        masterssdb "result.*rr_check_write"
        masterredis "rr_make_snapshot ok"
        masterredis "Sending rr_make_snapshot to SSDB"
        masterssdb "result.*rr_make_snapshot"
        masterredis "Sending rr_transfer_snapshot to SSDB"
        masterssdb "send rr_transfer_snapshot finished"
        masterredis "snapshot transfer finished"
        masterssdb "result.*rr_del_snapshot"
        masterredis "rr_del_snapshot ok"
    }
}

if {$::accurate} {
    set pairs $all
} else {
    set l [expr [llength $all]/2]
    set index [expr [randomInt $l]*2]
    set pairs [lrange $all $index [expr $index+1]]
}

foreach {flag pattern} $pairs {
    start_server {tags {"repl-abnormal"}} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set slaves {}
        start_server {} {
            set slave [srv 0 client]
            switch $flag {
                slaveredis {set log [srv 0 stdout]}
                masterredis {set log [srv -1 stdout]}
                slavessdb {set log [srv 0 ssdbstdout]}
                masterssdb {set log [srv -1 ssdbstdout]}
                default {fail "$flag is invaild"}
            }
            set num 100000
            if {$::accurate} {
                set clients 10
            } else {
                set clients 1
            }

            test "slave ssdb restart after $pattern" {
                set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                after 1000
                $slave slaveof $master_host $master_port
                wait_log_pattern $pattern $log

                kill_ssdb_server
                restart_ssdb_server
                wait_for_online $master 1

                stop_bg_client_list  $clist
            }

            test "master and slave are identical after $pattern (slave ssdb restart)" {
                # new client
                set slave [srv 0 client]
                wait_for_condition 100 100 {
                    [$master dbsize] == [$slave dbsize]
                } else {
                    fail "Different number of keys between master and slave after too long time."
                }
                #从节点ssdb发生重启的情况下不保证主从冷热数据分布一致
                #wait_for_condition 100 100 {
                #    [$master debug digest] == [$slave debug digest]
                #} else {
                #    fail "Different digest between master([$master debug digest]) and slave([$slave debug digest]) after too long time."
                #}
                assert {[$master dbsize] > 0}
                compare_debug_digest {0 -1}
            }
        }
    }
}

# redis和ssdb完成同步后运行过程中，发生重启
start_server {tags {"repl-abnormal"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set num 100000
        set clients 10
        set clist {}
        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
        after 500

        test "slave sync with master" {
            $slave slaveof $master_host $master_port
            wait_for_online $master
        }

        # TODO allow cold/hot keys distribute not identical under this condition
        # reason: real ops cached in slave_write_op_list, and storetossdb/dumpfromssdb not.
        # No need to resolve this as evaluated.
        test "slave ssdb restart during writing" {
            for {set n 0} {$n < 3} {incr n} {
                after 500
                kill_ssdb_server
                after 1000
                restart_ssdb_server
            }
            set slave [srv 0 client]
            set slavessdb [srv 0 ssdbclient]
            after 500
            stop_bg_client_list $clist
            wait_for_condition 100 100 {
                [$master dbsize] == [$slave dbsize]
            } else {
                fail "Different number of keys between master and slave after too long time."
            }
#            wait_for_condition 100 100 {
#                [$master debug digest] == [$slave debug digest]
#            } else {
#                fail "Different digest between master([$master debug digest]) and slave([$slave debug digest]) after too long time."
#            }
            assert_equal "" [status $master db0] "No keys should stay in redis"

#            从节点ssdb发生重启的情况下从节点ssdb中会出现脏数据情况
#            wait_for_condition 100 100 {
#                [$slavessdb ssdb_dbsize] == [status $slave keys_in_ssdb_count]
#            } else {
#                fail "slave ssdb keys num equal with ssdbkeys in redis:[$slavessdb ssdb_dbsize] != [status $slave keys_in_ssdb_count]"
#            }
            compare_debug_digest
        }
    }
}

return
# Not to verify identical between master and slave when master'ssdb restart.
start_server {tags {"repl-abnormal"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set slavessdb [srv 0 ssdbclient]
        set num 100000
        set clients 10
        set clist {}
        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
        after 500

        test "slave sync with master" {
            $slave slaveof $master_host $master_port
            wait_for_online $master
        }

        test "master ssdb restart during writing" {
            for {set n 0} {$n < 3} {incr n} {
                after 500
                kill_ssdb_server -1
                after 1000
                restart_ssdb_server -1
            }
            set master [srv -1 client]
            after 500
            stop_bg_client_list $clist
#            wait_for_condition 100 100 {
#                [$master dbsize] == [$slave dbsize]
#            } else {
#                fail "Different number of keys between master and slave after too long time."
#            }
#            wait_for_condition 100 100 {
#                [$master debug digest] == [$slave debug digest]
#            } else {
#                fail "Different digest between master([$master debug digest]) and slave([$slave debug digest]) after too long time."
#            }
#            assert_equal "" [status $master db0] "No keys should stay in redis"

#            wait_for_condition 100 100 {
#                [$slavessdb ssdb_dbsize] == [status $slave keys_in_ssdb_count]
#            } else {
#                fail "slave ssdb keys num equal with ssdbkeys in redis:[$slavessdb ssdb_dbsize] != [status $slave keys_in_ssdb_count]"
#            }

            compare_debug_digest
        }
    }
}
