proc randstring {min max {type binary}} {
    set len [expr {$min+int(rand()*($max-$min+1))}]
    set output {}
    if {$type eq {binary}} {
        set minval 0
        set maxval 255
    } elseif {$type eq {alpha}} {
        set minval 48
        set maxval 122
    } elseif {$type eq {compr}} {
        set minval 48
        set maxval 52
    }
    while {$len} {
        append output [format "%c" [expr {$minval+int(rand()*($maxval-$minval+1))}]]
        incr len -1
    }
    return $output
}

# Useful for some test
proc zlistAlikeSort {a b} {
    if {[lindex $a 0] > [lindex $b 0]} {return 1}
    if {[lindex $a 0] < [lindex $b 0]} {return -1}
    string compare [lindex $a 1] [lindex $b 1]
}

# Return all log lines starting with the first line that contains a warning.
# Generally, this will be an assertion error with a stack trace.
proc warnings_from_file {filename} {
    set lines [split [exec cat $filename] "\n"]
    set matched 0
    set logall 0
    set result {}
    foreach line $lines {
        if {[string match {*REDIS BUG REPORT START*} $line]} {
            set logall 1
        }
        if {[regexp {^\[\d+\]\s+\d+\s+\w+\s+\d{2}:\d{2}:\d{2} \#} $line]} {
            set matched 1
        }
        if {$logall || $matched} {
            lappend result $line
        }
    }
    join $result "\n"
}

# Return value for INFO property
proc status {r property} {
    if {[regexp "\r\n$property:(.*?)\r\n" [{*}$r info] _ value]} {
        set _ $value
    }
}

proc waitForBgsave r {
    while 1 {
        if {[status r rdb_bgsave_in_progress] eq 1} {
            if {$::verbose} {
                puts -nonewline "\nWaiting for background save to finish... "
                flush stdout
            }
            after 1000
        } else {
            break
        }
    }
}

proc waitForBgrewriteaof r {
    while 1 {
        if {[status $r aof_rewrite_in_progress] eq 1} {
            if {$::verbose} {
                puts -nonewline "\nWaiting for background AOF rewrite to finish... "
                flush stdout
            }
            after 1000
        } else {
            break
        }
    }
}

proc wait_for_sync r {
    while 1 {
        if {[status $r master_link_status] eq "down"} {
            after 10
        } else {
            break
        }
    }
}

proc wait_for_online {r {num 1} {retry 500}} {
    incr num -1
    # set retry 500
    set pattern "*"
    for {set n 0} {$n <= $num} {incr n 1} {
        append pattern "slave$n:*state=online*"
    }
    while {$retry} {
        set info [$r info]
        if {[string match $pattern $info]} {
            break
        } else {
            incr retry -1
            after 100
        }
    }
    if {$retry == 0} {
        error "assertion:Slaves not correctly synchronized"
    }
    return [expr (500 - $retry)*0.1]
}

# Random integer between 0 and max (excluded).
proc randomInt {max} {
    expr {int(rand()*$max)}
}

# Random signed integer between -max and max (both extremes excluded).
proc randomSignedInt {max} {
    set i [randomInt $max]
    if {rand() > 0.5} {
        set i -$i
    }
    return $i
}

proc randpath args {
    set path [expr {int(rand()*[llength $args])}]
    uplevel 1 [lindex $args $path]
}

proc randomValue {} {
    randpath {
        # Small enough to likely collide
        randomSignedInt 1000
    } {
        # 32 bit compressible signed/unsigned
        randpath {randomSignedInt 2000000000} {randomSignedInt 4000000000}
    } {
        # 64 bit
        randpath {randomSignedInt 1000000000000}
    } {
        # Random string
        randpath {randstring 0 256 alpha} \
                {randstring 0 256 compr} \
                {randstring 0 256 binary}
    }
}

proc randomKey {} {
    randpath {
        # Small enough to likely collide
        randomInt 1000
    } {
        # 32 bit compressible signed/unsigned
        randpath {randomInt 2000000000} {randomInt 4000000000}
    } {
        # 64 bit
        randpath {randomInt 1000000000000}
    }
#    {
#        # Random string
#        randpath {randstring 1 256 alpha} \
#                {randstring 1 256 compr}
#    }
}

proc findKeyWithType {r type} {
    for {set j 0} {$j < 20} {incr j} {
        set k [{*}$r randomkey]
        if {$k eq {}} {
            return {}
        }
        if {[{*}$r type $k] eq $type} {
            return $k
        }
    }
    return {}
}

proc createComplexDataset {r ops {opt {}}} {
    set keyslist {}
    for {set j 0} {$j < $ops} {incr j} {
        # TODO grace quit clients ops
        if {$j%100 == 0} {
            if {[$r get stopflag] eq "true"} {
                break
            }
        }
        if {[lsearch -exact $opt samekey] != -1} {
            set k "key_8888888888"
        } elseif {[lsearch -exact $opt incr] != -1} {
            $r incr incr_key
            continue
         } else {
            randpath {
                set k [randomKey]
            } {
                set k [lindex $keyslist [randomInt [llength $keyslist]]]
            }
        }
        lappend keyslist $k
        set f [randomValue]
        if {[lsearch -exact $opt 10M] != -1} {
            set v [string repeat x 10000000]
        } elseif {[lsearch -exact $opt 1M] != -1} {
            set v [string repeat x 1000000]
        } elseif {[lsearch -exact $opt 100k] != -1} {
            set v [string repeat x 100000]
        } elseif {[lsearch -exact $opt 10k] != -1} {
            set v [string repeat x 100000]
        } elseif {[lsearch -exact $opt 1k] != -1} {
            set v [string repeat x 1000]
        } else {
            set v [randomValue]
        }

        set d [randomSignedInt 100000000]
        catch {
            set t [{*}$r type $k]
            if {$t eq {none}} {
                randpath {
                    {*}$r set $k $v
                } {
                    {*}$r lpush $k $v
                } {
                    {*}$r sadd $k $v
                } {
                    {*}$r zadd $k $d $v
                } {
                    {*}$r hset $k $f $v
                } {
                    {*}$r del $k
                }
                set t [{*}$r type $k]
            }
        } err;# WRONGTYPE ERR

        catch {
            switch $t {
                {string} {
                    randpath {{*}$r set $k $v} \
                    {{*}$r set $k ${v}_${d}}
                }
                {list} {
                    randpath {{*}$r lpush $k $v} \
                    {{*}$r rpush $k $v} \
                    {{*}$r rpush $k ${v}_${d}} \
                    {{*}$r rpop $k} \
                    {{*}$r lpop $k}
                }
                {set} {
                    randpath {{*}$r sadd $k $v} \
                    {{*}$r sadd $k ${v}_${d}} \
                    {{*}$r srem $k $v}
                }
                {zset} {
                    randpath {{*}$r zadd $k $d $v} \
                    {{*}$r zadd $k $d ${v}_${d}} \
                    {{*}$r zrem $k $v}
                }
                {hash} {
                    randpath {{*}$r hset $k $f $v} \
                    {{*}$r hset $k $f ${v}_${d}} \
                    {{*}$r hdel $k $f}
                }
            }
        } err;# WRONGTYPE ERR

        if {[lsearch -exact $opt useexpire] != -1} {
            if {rand() < 0.5} {
                if {[lsearch -exact $opt noexpired] != -1} {
                    set delaytime 1000
                } else {
                    set delaytime 0
                }
               randpath {{*}$r pexpire $k [expr 1000*$delaytime+[randomInt 10]]} \
                       {{*}$r expire $k [expr $delaytime+[randomInt 3]]} \
                       {{*}$r expireat $k [expr $delaytime+[clock seconds] +[randomInt 3]]} \
                       {{*}$r pexpireat $k [expr 1000*$delaytime+[clock milliseconds] +[randomInt 1000]]} \
                       {{*}$r expire $k [expr $delaytime+[randomInt 3]]}
                if {{string} eq $t} {
                    catch {
                        randpath {{*}$r setex $k [expr $delaytime+[randomInt 3]] ${v}_volatile} \
                        {{*}$r set $k ${v}_volatile EX [expr $delaytime+[randomInt 3]]}
                        {{*}$r set $k ${v}_volatile PX [expr 1000*$delaytime+[randomInt 1000]]}
                   } err;
                }
            }
        }
    }
    return [lsort -unique $keyslist ]
}

proc formatCommand {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args {
        append cmd "$[string length $a]\r\n$a\r\n"
    }
    set _ $cmd
}

proc csvdump r {
    set o {}
    for {set db 0} {$db < 16} {incr db} {
        {*}$r select $db
        foreach k [lsort [{*}$r keys *]] {
            set type [{*}$r type $k]
            append o [csvstring $db] , [csvstring $k] , [csvstring $type] ,
            switch $type {
                string {
                    append o [csvstring [{*}$r get $k]] "\n"
                }
                list {
                    foreach e [{*}$r lrange $k 0 -1] {
                        append o [csvstring $e] ,
                    }
                    append o "\n"
                }
                set {
                    foreach e [lsort [{*}$r smembers $k]] {
                        append o [csvstring $e] ,
                    }
                    append o "\n"
                }
                zset {
                    foreach e [{*}$r zrange $k 0 -1 withscores] {
                        append o [csvstring $e] ,
                    }
                    append o "\n"
                }
                hash {
                    set fields [{*}$r hgetall $k]
                    set newfields {}
                    foreach {k v} $fields {
                        lappend newfields [list $k $v]
                    }
                    set fields [lsort -index 0 $newfields]
                    foreach kv $fields {
                        append o [csvstring [lindex $kv 0]] ,
                        append o [csvstring [lindex $kv 1]] ,
                    }
                    append o "\n"
                }
            }
        }
    }
    {*}$r select 0
    # {*}$r select 9
    return $o
}

proc csvstring s {
    return "\"$s\""
}

proc roundFloat f {
    format "%.10g" $f
}

proc find_available_port start {
    for {set j $start} {$j < $start+1024} {incr j} {
        if {[catch {set fd1 [socket 127.0.0.1 $j]}] &&
            [catch {set fd2 [socket 127.0.0.1 [expr $j+10000]]}] &&
            [catch {set fd3 [socket 127.0.0.1 [expr $j+$::ssdbport]]}]} {
            return $j
        } else {
            catch {
                close $fd1
                close $fd2
                close $fd3
            }
        }
    }
    if {$j == $start+1024} {
        error "Can't find a non busy port in the $start-[expr {$start+1023}] range."
    }
}

# Test if TERM looks like to support colors
proc color_term {} {
    expr {[info exists ::env(TERM)] && [string match *xterm* $::env(TERM)]}
}

proc colorstr {color str} {
    if {[color_term]} {
        set b 0
        if {[string range $color 0 4] eq {bold-}} {
            set b 1
            set color [string range $color 5 end]
        }
        switch $color {
            red {set colorcode {31}}
            green {set colorcode {32}}
            yellow {set colorcode {33}}
            blue {set colorcode {34}}
            magenta {set colorcode {35}}
            cyan {set colorcode {36}}
            white {set colorcode {37}}
            default {set colorcode {37}}
        }
        if {$colorcode ne {}} {
            return "\033\[$b;${colorcode};49m$str\033\[0m"
        }
    } else {
        return $str
    }
}

# Execute a background process writing random data for the specified number
# of seconds to the specified Redis instance.
proc start_write_load {host port seconds} {
    set tclsh [info nameofexecutable]
    exec $tclsh tests/helpers/gen_write_load.tcl $host $port $seconds &
}

proc start_hit_ssdb_tps {host port seconds} {
    set tclsh [info nameofexecutable]
    exec $tclsh tests/helpers/start_hit_ssdb_tps.tcl $host $port $seconds &
}

# Stop a process generating write load executed with start_write_load.
proc stop_write_load {handle} {
    catch {exec /bin/kill -9 $handle}
}

#TODO
proc OnRead {fd} {
 if {[eof $fd] } {
        puts "Terminated."
        if {$::mirror == 0} {
            set ::mirror 1
        }
        close $fd
    } else {
        set buf [gets $fd]
        if {[regexp {1000} $buf]} {
            puts "buf:$buf"
        }
    }
}

proc start_bg_mirror_complex {host port shost sport ops {opt {}}} {
    set ::mirror 1
    set tclsh [info nameofexecutable]
    exec $tclsh tests/helpers/bg_mirror_complex.tcl $host $port $shost $sport $ops $opt &
#    set fd [open "|$tclsh tests/helpers/bg_mirror_complex.tcl $host $port $shost $sport $ops $opt"]
#    fileevent $fd readable [list OnRead $fd]
#    return $fd
}

#TODO
proc stop_bg_mirror_complex {} {
    set ::mirror 0
    vwait ::mirror
    puts mirror:"$::mirror"
}

proc start_bg_complex_data {host port db ops {opt {}}} {
    set tclsh [info nameofexecutable]
    set db 0
    exec $tclsh tests/helpers/bg_complex_data.tcl $host $port $db $ops $opt &
}

proc stop_bg_complex_data {handle} {
    catch {exec /bin/kill -9 $handle}
}

proc start_bg_complex_data_list {host port num clients {opt {}}} {
    set clientlist {}
    for {set n 0} {$n < $clients} {incr n} {
        lappend clientlist [start_bg_complex_data $host $port 0 $num $opt]
    }
    return $clientlist
}

proc start_bg_command_list {host port {clients 1} args} {
    set tclsh [info nameofexecutable]
    # set clients 2
    set clientlist {}
    for {set n 0} {$n < $clients} {incr n} {
        lappend clientlist [exec $tclsh tests/helpers/bg_command.tcl $host $port {*}$args &]
    }
    return $clientlist
}

proc stop_bg_client_list {clientlist} {
    foreach client $clientlist {
        catch {exec /bin/kill -9 $client}
    }
}

proc stop_index_bg_complex_data_in_list {clientlist n} {
    assert {$n < [llength $clientlist]}
    catch {exec /bin/kill -9 [lindex $clientlist $n]}
    # remove the killed handle from list
    set clientlist [ lreplace $clientlist $n $n ]
    return $clientlist
}

proc wait_for_dumpto_ssdb {r key} {
    #wait key dumped to ssdb
    wait_for_condition 500 1 {
        [ $r locatekey $key ] eq {ssdb}
    } else {
        fail "key $key be storetossdb failed"
    }
}

proc wait_for_restoreto_redis {r key} {
    #wait key restore to redis
    $r dumpfromssdb $key ;# TODO
    wait_for_condition 100 1 {
        [$r locatekey $key] eq {redis}
    } else {
        fail "key $key restore redis failed"
    }
}

proc dumpto_ssdb_and_wait {r key} {
    $r storetossdb $key
    #wait key dumped to ssdb: 100ms -> 500ms for bighash dump timeout
    wait_for_condition 500 1 {
        [ $r locatekey $key ] eq {ssdb}
    } else {
        fail "key $key be storetossdb failed"
    }
}

proc ssdbr {args} {
    if {"object" eq [lindex $args 0] && "encoding" eq [lindex $args 1]} {
        wait_for_restoreto_redis r [lindex $args 2]
    } elseif {"object" eq [lindex $args 0] && "refcount" eq [lindex $args 1]} {
        wait_for_restoreto_redis r [lindex $args 2]
    } elseif {"debug" eq [lindex $args 0] && "object" eq [lindex $args 1]} {
        wait_for_restoreto_redis r [lindex $args 2]
    } elseif {"redis" eq [ r locatekey [lindex $args 1] ]} {
        dumpto_ssdb_and_wait r [lindex $args 1]
    }
    r {*}$args
}

proc wait_memory_stable {{level 0}} {
    set current_mem 0

    set retry 200
    while {[s $level used_memory] != $current_mem} {
        set current_mem [s $level used_memory]
        incr retry -1
        after 100
        if {$retry == 0} {
            puts "assertion:wait memory stable 20s"
            break
        }
    }
}

# flag = 1 mean that wait memory used upper than transfer_limit.
# flag = 0 mean that wait memory used lower than transfer_limit and keep stable.
proc wait_for_transfer_limit {flag {level 0}} {
    set maxmemory [lindex [r $level config get maxmemory] 1 ]
    set ssdb_transfer_limit [lindex [r $level config get ssdb-transfer-lower-limit] 1]
    assert {$maxmemory ne 0}
    set current_mem 0
    if {1 == $flag} {
        while {[s $level used_memory] < [expr $maxmemory*$ssdb_transfer_limit/100.0]} {
            after 100
        }
    } elseif {0 == $flag} {
        while {[s $level used_memory] > [expr $maxmemory*$ssdb_transfer_limit/100.0]\
        || [s $level used_memory] != $current_mem} {
            set current_mem [s $level used_memory]
            after 100
        }
    }
}

# num: populate different nums in redis or ssdb, unlimited when == -1
proc populate_diff_keys {r2 r1 {num -1} {flag redis}} {
    if {$flag == "ssdb"} {
        $r1 select 16
        $r2 select 16
    }
    # $r1 config set jdjr-mode no
    # $r2 config set jdjr-mode no
    set list1 [lsort [$r1 keys *]]
    set list2 [lsort [$r2 keys *]]
    set len1 [llength $list1]
    set len2 [llength $list2]
    set diff {}
    set i 0
    set j 0
    while {$i < $len1 && $j < $len2 && $num != 0} {
        if {[ string compare [lindex $list1 $i] [lindex $list2 $j] ] == -1} {
            lappend diff [lindex $list1 $i]
            incr num -1
            incr i
        } elseif {[ string compare [lindex $list1 $i] [lindex $list2 $j] ] == 1} {
            lappend diff [lindex $list2 $j]
            incr num -1
            incr j
        } else {
            if {[ string compare [$r1 dump [lindex $list1 $i]] [$r2 dump [lindex $list2 $j]]] != 0} {
                lappend diff [lindex $list2 $i]
                incr num -1
            }
            incr i
            incr j
        }
    }
    if {$flag == "ssdb"} {
        $r1 select 0
        $r2 select 0
    }
    # $r1 config set jdjr-mode yes
    # $r2 config set jdjr-mode yes

    while {$i < $len1 && $num !=0} {
        lappend diff [lindex $list1 $i]
        incr num -1
        incr i
    }

    while {$j < $len2 && $num !=0} {
        lappend diff [lindex $list2 $j]
        incr num -1
        incr j
    }
    return $diff
}

proc debug_digest {r {level 0}} {
    set oldmemory [lindex [$r $level config get maxmemory] 1 ]
    # avoid keys transfer to ssdb again after debug digest.
    if {[lindex [$r $level role] 0] == "master"} {
        assert_equal 0 $oldmemory "maxmemory should be 0 for all keys load to redis"
        set retry 10
        while {$retry} {
            set keyslist [$r $level ssdbkeys *]
            if {[llength $keyslist] == 0} {break}
            foreach key $keyslist {
                # some key hot in master, cold in slave, and some are opposite.
                $r $level dumpfromssdb $key
                # $r $level exists $key ;#load keys to redis
            }
            after 1000
            incr retry -1
        }
        if {$retry == 0} {
            error "assertion:master not load all keys after multi access key"
        }

        # TODO sometimes key in redis of master, but in ssdb of slave. so the ssdbkey not null in slave.
        foreach ssdbkey [$r $level ssdbkeys *] {
            assert_equal 0 [ $r $level exists $ssdbkey ] "key: $ssdbkey should not in ssdbkeys."
        }

        wait_for_condition 200 100 {
            [s $level keys_in_ssdb_count] eq 0
        } else {
            fail "wait $r $level debug digest timeout."
        }
    }


    set digest [$r $level debug digest]
    return $digest
}

proc is_ssdb_alive {{level 0}} {
    set config [lindex $::servers end+$level]
    set ssdbpid [dict get $config ssdbpid]
    if {[catch {exec ps -p $ssdbpid} err]} {
        return 0
    } else {
        return 1
    }
}

proc kill_ssdb_server {{level 0}} {
    set config [lindex $::servers end+$level]
    # nothing to kill when running against external server
    if {$::external} return

    # nevermind if its already dead
    if {![is_ssdb_alive $level]} { return }
    set ssdbpid [dict get $config ssdbpid]

    # kill ssdb server and wait for the process to be totally exited
    catch {exec kill -9 $ssdbpid}
    if {$::valgrind} {
        set max_wait 60000
    } else {
        set max_wait 10000
    }
    while {[is_ssdb_alive $level]} {
        incr wait 10

        if {$wait >= $max_wait} {
            puts "Forcing process $ssdbpid to exit..."
            catch {exec kill -KILL $ssdbpid}
        } elseif {$wait % 1000 == 0} {
            puts "Waiting for process $ssdbpid to exit..."
        }
        after 10
    }

    # Check valgrind errors if needed
    if {$::valgrind} {
        check_valgrind_errors [dict get $config ssdbstderr]
    }
}

proc wait_log_pattern {pattern log} {
    set retry 10000
    set debuglog {
        "receive.*rr_check_write"
        "result.*rr_check_write"
        "rr_make_snapshot ok"
        "Sending rr_make_snapshot to SSDB"
        "result.*rr_make_snapshot"
        "Sending rr_transfer_snapshot to SSDB"
        "send rr_transfer_snapshot finished"
        "snapshot transfer finished"
        "result.*rr_del_snapshot"
        "rr_del_snapshot ok"
    }
    if {$::loglevel != "debug" && [lsearch $debuglog $pattern] != -1 } {
        set delay [randomInt 100]
        after $delay
        set retry [expr $retry-$delay]
    } else {
        while {$retry} {
            catch {[exec grep $pattern $log | wc -l]} err
            if {![string match "*child process*" $err]} {
                break
            }
            incr retry -1
            after 1
        }
    }
    if {$retry == 0} {
        error "assertion:expected log \"$pattern\" not found on log file"
    } else {
        puts "wait [expr 10000-$retry]ms for $pattern!"
    }
}

proc wait_start_ssdb_server {{level 0}} {
    set config [lindex $::servers end+$level]
    set ssdbstdout [dict get $config "ssdbstdout"]
    wait_log_pattern "ssdb server started" $ssdbstdout
    after 1000
}

proc restart_ssdb_server {{level 0}} {
    set config [lindex $::servers end+$level]
    set host [dict get $config "host"]
    set port [dict get $config "port"]
    set ssdb_config_file [dict get $config ssdb_config_file]
    set ssdbstdout [dict get $config ssdbstdout]
    set ssdbstderr [dict get $config ssdbstderr]
    exec cat $ssdbstdout >> $ssdbstderr
    set ssdbpid [exec ssdb-server $ssdb_config_file > $ssdbstdout 2>> $ssdbstderr &]

    wait_start_ssdb_server $level
    send_data_packet $::test_server_fd server-spawned $ssdbpid
    dict set config "ssdbpid" $ssdbpid
    # reconnect new client after restart ssdb
    set client [redis $host $port]
    dict set config "client" $client
    set ssdbclient [redis $host [expr $port+$::ssdbport]]
    dict set config "ssdbclient" $ssdbclient

    # re-set withe new ssdbpid in the servers list
    lset ::servers end+$level $config
}

proc wait_ssdb_reconnect {{level 0}} {
    set retry 10000
    r $level set foonull null
    while {$retry} {
        catch {[r $level storetossdb foonull]} err
        if {![string match "*ERR*disconnected*" $err]} {
            break
        }
        incr retry -1
        after 1
    }
    if {$retry == 0} {
        error "timeout for ssdb reconnect"
    } else {
        puts "wait [expr 10000-$retry]ms for ssdb reconnect!"
    }
    for {set n 0} {$n < 10} {incr n} {
        catch {[r $level del foonull]} ret
        if {![string match "*ERR*timeout*" $err]} {
            break
        }
        after 1000
    }
}

proc wait_flushall {{level 0}} {
    set config [lindex $::servers end+$level]
    set stdout [dict get $config "stdout"]
    wait_log_pattern "rr_do_flushall ok" $stdout
}

proc wait_keys_processed {r {levels {0}}} {
    foreach level $levels {
        wait_for_condition 100 200 {
            [s $level "keys_loading_from_ssdb"] == 0 &&
            [s $level "keys_transferring_to_ssdb"] == 0 &&
            [s $level "keys_visiting_ssdb"] == 0
        } else {
            fail "some keys not process done after 10s"
        }
        if {[lindex [$r $level role] 0] == "slave"} {
            wait_for_condition 100 100 {
                [ s $level "slave_unprocessed_transferring_or_loading_keys" ] == 0 &&
                [ s $level "slave_write_op_list_num" ] == 0 &&
                [ s $level "slave_write_op_list_memsize" ] == 0 &&
                [ s $level "slave_ssdb_critical_write_error_count" ] == 0
            } else {
                fail "some keys not transfer/load done in slave"
            }
        }
    }
}

proc check_keys_cleared {r {levels {0}}} {
    foreach level $levels {
        wait_for_condition 50 100 {
            [s $level "keys_in_redis_count"] == 0 &&
            [s $level "keys_in_ssdb_count"] == 0 &&
            [s $level "keys_loading_from_ssdb"] == 0 &&
            [s $level "keys_transferring_to_ssdb"] == 0 &&
            [s $level "keys_visiting_ssdb"] == 0
        } else {
            fail "some keys not clear after 10s"
        }
        if {[lindex [$r $level role] 0] == "slave"} {
            wait_for_condition 50 100 {
                [ s $level "slave_unprocessed_transferring_or_loading_keys"] == 0 &&
                [ s $level "slave_write_op_list_num" ] == 0 &&
                [ s $level "slave_write_op_list_memsize" ] == 0 &&
                [ s $level "slave_ssdb_critical_write_error_count" ] == 0
            } else {
                fail "some keys not clear in slave($level)"
            }
        }
    }
}

# ignore only index not identical.
proc check_real_diff_keys {master slaves} {
    foreach slave $slaves {
        set diffkeys [populate_diff_keys $master $slave]
        foreach key $diffkeys {
            if {[$master exists $key] != [$slave exists $key]} {
                fail "$key not identical!"
            }
        }
    }
}

proc access_key_tps_time {key tps time {wait true} {level 0}} {
    set begin [clock milliseconds]
    set n 0
    # update [clock milliseconds] to cur.
    set cur [clock milliseconds]
    while {[expr ($cur - $begin)/1000 < $time]} {
        if {[expr ($cur - $begin)/1000 >= $n]} {
            for {set i 0} {$i < $tps} {incr i} {
                r $level exists $key
            }
            incr n
            # break out when reach tps if wait == true
            if {false == $wait && $n == $time} {
                break
            }
            if {$key == "keynull"} {
                r $level setlfu $key 0
            }
        }
        after 1
        set cur [clock milliseconds]
    }
}

proc hit_ssdb_load_key {{level 0}} {
    r $level set keynull 0
    r $level storetossdb keynull
    set rule [lindex [r $level config get ssdb-load-rule] 1]
    lassign $rule time tps
    access_key_tps_time keynull [expr $tps/$time*1.1] $time true $level
}

proc get_val_with_types {level key} {
    switch [r $level type $key] {
        {string} {
            set vlist [list [r $level get $key]]
        }
        {list} {
            set vlist [lsort [r $level lrange $key 0 -1]]
        }
        {set} {
            set vlist [lsort [r $level smembers $key]]
        }
        {zset} {
            set vlist [lsort [r $level zrange $key 0 -1 withscores]]
        }
        {hash} {
            set vlist [lsort [r $level hgetall $key]]
        }
        {none} {
            set vlist ""
        }
    }
    return $vlist
}

proc compare_key {key {levels {0 -1}}} {
    assert {[llength $levels] > 1}


    set val [get_val_with_types [lindex $levels 0] $key]

    foreach level [lrange $levels 1 end] {

        # assert_equal {redis} [r $level locatekey $key] "key: $key should be in redis."

        foreach v1 $val v2 [get_val_with_types $level $key] {
            assert_equal $v1 $v2 "key: $key not identical between level [lindex $levels 0] and $level."
        }
    }
}

proc compare_allkeys {{levels {0 -1}}} {
    set keyslist [r [lindex $levels 0] rediskeys *]
    foreach key $keyslist {
        compare_key $key $levels
    }
}

proc compare_debug_digest {{levels {0 -1}}} {
    assert {[llength $levels] > 1}
    set master_level 1
    set master_digest {}
    set master_memory 0
    foreach level $levels {
        if {[lindex [r $level role] 0] == "master"} {
            set master_level $level
            break
        }
    }

    if {$master_level ne 1} {
        set master_memory [lindex [r $master_level config get maxmemory] 1 ]
        r $master_level config set maxmemory 0
        set master_digest [debug_digest r $master_level]
    } else {
        fail "no master cannot debug digest!!"
    }

    foreach level $levels {
        if {$master_level ne $level} {
            if {$master_digest ne [debug_digest r $level]} {
                puts "master digest should equal to each slave!!!!!"
                compare_allkeys [list $master_level $level]
            }
            # assert_equal $master_digest [debug_digest r $level] "master digest should equal to each slave"
        }
    }
    r $master_level config set maxmemory $master_memory
}
