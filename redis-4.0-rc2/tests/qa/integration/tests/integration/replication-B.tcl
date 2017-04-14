# abnormal case
# redis和ssdb都开始传输快照后，如果ssdb挂掉，没有收到ssdb的“快照传输完成”的响应。
start_server {tags {"repl-abnormal"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set num 10000
        set clients 1
        set clist {}
        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
        after 1000

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
                stop_bg_client_list $clist
                fail "slave can not sync with master during writing"
            }
            wait_for_online $master
            stop_bg_client_list $clist
            compare_debug_digest
        }

        test "complex ops after restore sync from ssdb restart" {
            set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
            after 1000
            stop_bg_client_list $clist
            compare_debug_digest
        }
    }
}

# ssdb开始复制数据时，snapshot为空（redis未发送rr_make_snapshot），返回redis错误
# 即在ssdb  make snapshot完成后重启ssdb清空snapshot。
start_server {tags {"repl-abnormal"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set num 10000
        set clients 1
        set clist {}
        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
        after 1000

        test "slave ssdb restart when master start to transfer snapshot" {
            $slave slaveof $master_host $master_port
            set pattern "result.*rr_make_snapshot"
            wait_log_pattern $pattern [srv -1 ssdbstdout]

            kill_ssdb_server -1
            restart_ssdb_server -1
            set slave [srv 0 client]

            wait_for_condition 50 500 {
                [lindex [$slave role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$slave info replication]]
            } else {
                stop_bg_client_list $clist
                fail "slave can not sync with master during writing"
            }
            wait_for_online $master
            stop_bg_client_list $clist
            compare_debug_digest
        }

        test "complex ops after restore sync from ssdb restart" {
            set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
            after 1000
            stop_bg_client_list $clist
            compare_debug_digest
        }
    }
}

 ssdb做快照后，如果redis挂掉并且没有发送rr_transfer_snapshot的时候，ssdb要处理这种异常，释放快照。
 redis挂掉不监控重启,在集群中测试其failover
start_server {tags {"repl-abnormal"}} {
    start_server {} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set num 10000
        set clients 0
        set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
        after 1000

        test "master redis down when ssdb make snapshot" {
            $slave slaveof $master_host $master_port
            set pattern "receive.*rr_make_snapshot"
            wait_log_pattern $pattern [srv -1 ssdbstdout]
            catch {exec kill [srv -1 pid]}

            # TODO verify identical
            stop_bg_client_list $clist
        }
    }
}
# 如果在快照删除的超时时间内redis又发了rr_make_snapshot，ssdb也要释放前一次的快照并重新生成，即保证ssdb中只存在一份快照。
start_server {tags {"repl-abnormal"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set slaves {}
    start_server {} {
        lappend slaves [srv 0 client]
        start_server {} {
            lappend slaves [srv 0 client]
            set num 10000
            set clients 1
            set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
            after 1000

            test "make snapshot again during deleting snapshot" {
                [lindex $slaves 0] slaveof $master_host $master_port
                set pattern "send rr_transfer_snapshot finished"
                wait_log_pattern $pattern [srv -2 ssdbstdout]
                [lindex $slaves 1] slaveof $master_host $master_port
                wait_for_online $master 2

                stop_bg_client_list $clist
                compare_debug_digest [list 0 -1 -2]
            }
        }
    }
}
# 如果刚好加载/转存ssdb key时redis挂了，导致redis的db[0]和evictdb中同时存在一个key的情况。
# TODO
# redis client double free问题。
# TODO

## 在主从复制的任一阶段，将redis或ssdb搞挂，恢复后是否还能重新发起主从复制。
foreach {log pattern} {slaveredis "SLAVE OF.*enabled" 
    slaveredis "processing slaveof" 
    slaveredis "Connecting to MASTER" 
    masterredis "Sending rr_check_write to SSDB"
    masterssdb "receive.*rr_check_write"
    masterssdb "result.*rr_check_write"
    masterredis "Sending rr_make_snapshot to SSDB"
    masterssdb "receive.*rr_make_snapshot"
    slaveredis "MASTER <-> SLAVE sync started"
    masterssdb "result.*rr_make_snapshot"
    masterredis "Starting BGSAVE for SYNC with target"
    slaveredis "Full resync from master"
    masterredis "DB saved on disk"
    masterredis "Background saving terminated"
    masterssdb "receive.*rr_transfer_snapshot"
    masterssdb "result.*rr_transfer_snapshot"
    slaveredis "MASTER <-> SLAVE sync: Flushing old data"
    slaveredis "MASTER <-> SLAVE sync: Finished with success"
    slavessdb "reply replic finish ok"
    masterssdb "send rr_transfer_snapshot finished"
    masterssdb "replic procedure finish"
    masterredis "processing replconf"
    masterssdb "result.*rr_del_snapshot"
} {
    start_server {tags {"repl-abnormal"}} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set slaves {}
        # start_server {}
            # lappend slaves [srv 0 client]
            start_server {} {
                switch $log {
                    slaveredis {set log [srv 0 stdout]}
                    masterredis {set log [srv -1 stdout]}
                    slavessdb {set log [srv 0 ssdbstdout]}
                    masterssdb {set log [srv -1 ssdbstdout]}
                    default {fail "$log is invaild"}
                }
                lappend slaves [srv 0 client]
                set num 10000
                set clients 1
                set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                # after 1000

                test "slave redis down after $pattern" {
                    [lindex $slaves 0] slaveof $master_host $master_port
                    # [lindex $slaves 1] slaveof $master_host $master_port
                    after 1000000
                    wait_log_pattern $pattern $log

                    puts "$pattern"
                    # kill_ssdb_server -1
                    # restart_ssdb_server -1
                    # wait_for_online $master 2

                    # TODO verify identical
                    stop_bg_client_list  $clist
                }
                # after 1000000
            }
        }
    # {}
}
