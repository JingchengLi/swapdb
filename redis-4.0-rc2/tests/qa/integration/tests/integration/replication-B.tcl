# abnormal case
# ssdb开始复制数据时，snapshot为空（redis未发送rr_make_snapshot），返回redis错误
# 即在ssdb  make snapshot完成后重启ssdb清空snapshot。
# This case included in below

# ssdb做快照后，如果redis挂掉并且没有发送rr_transfer_snapshot的时候，ssdb要处理这种异常，释放快照。
# redis挂掉不监控重启,在集群中测试其failover

# 如果刚好加载/转存ssdb key时redis挂了，导致redis的db[0]和evictdb中同时存在一个key的情况。
# TODO
# redis client double free问题。
# TODO

## 在主从复制的任一阶段，将redis或ssdb搞挂，恢复后是否还能重新发起主从复制。
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
            lappend slaves [srv 0 client]
            start_server {} {
                lappend slaves [srv 0 client]
                switch $flag {
                    slaveredis {set log [srv -1 stdout]}
                    masterredis {set log [srv -2 stdout]}
                    slavessdb {set log [srv -1 ssdbstdout]}
                    masterssdb {set log [srv -2 ssdbstdout]}
                    default {fail "$flag is invaild"}
                }
                set num 100000
                if {$::accurate} {
                    set clients 10
                } else {
                    set clients 1
                }

                test "master ssdb restart after $pattern" {
                    set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                    after 1000
                    [lindex $slaves 0] slaveof $master_host $master_port
                    wait_log_pattern $pattern $log

                    kill_ssdb_server -2
                    restart_ssdb_server -2
                    # new client
                    set master [srv -2 client]
                    wait_for_online $master 1

                    stop_bg_client_list  $clist
                }

                test "master and slave are identical after $pattern (master ssdb restart)" {
                    wait_for_condition 100 100 {
                        [$master dbsize] == [[lindex $slaves 0] dbsize]
                    } else {
                        fail "Different number of keys between master and slave after too long time."
                    }
                    wait_for_condition 100 100 {
                        [$master debug digest] == [[lindex $slaves 0] debug digest]
                    } else {
                        fail "Different digest between master([$master debug digest]) and slave([[lindex $slaves 0] debug digest]) after too long time."
                    }
                    assert {[$master dbsize] > 0}
                    compare_debug_digest {-1 -2}
                }

                switch $flag {
                    slaveredis {set log [srv 0 stdout]}
                    masterredis {set log [srv -2 stdout]}
                    slavessdb {set log [srv 0 ssdbstdout]}
                    masterssdb {set log [srv -2 ssdbstdout]}
                    default {fail "$flag is invaild"}
                }

                # cleanup the log during first slaveof and backup it
                set out_bak [format "%s_bak" $log]
                exec cp $log $out_bak
                exec echo > $log

                test "slave ssdb restart after $pattern" {
                    set clist [ start_bg_complex_data_list $master_host $master_port $num $clients ]
                    after 1000
                    [lindex $slaves 1] slaveof $master_host $master_port
                    wait_log_pattern $pattern $log

                    kill_ssdb_server
                    restart_ssdb_server
                    # new client
                    set slave [srv 0 client]
                    wait_for_online $master 2

                    stop_bg_client_list  $clist
                }

                test "master and slave are identical after $pattern (slave ssdb restart)" {
                    wait_for_condition 100 100 {
                        [$master dbsize] == [$slave dbsize]
                    } else {
                        fail "Different number of keys between master and slave after too long time."
                    }
                    wait_for_condition 100 100 {
                        [$master debug digest] == [$slave debug digest]
                    } else {
                        fail "Different digest between master([$master debug digest]) and slave([$slave debug digest]) after too long time."
                    }
                    assert {[$master dbsize] > 0}
                    compare_debug_digest {0 -1 -2}
                }
            }
        }
    }
}
