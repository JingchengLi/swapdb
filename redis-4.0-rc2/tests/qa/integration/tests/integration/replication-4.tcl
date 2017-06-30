start_server {tags {"repl"}} {
    start_server {} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set num 10000
        set clients 50
        test {#issue master and slave may be identical when write clients killed forcely} {
            $slave slaveof $master_host $master_port
            wait_for_online $master
            after 1000
            set clist [ start_bg_complex_data_list $master_host $master_port $num $clients incr]
            after 100
            stop_bg_client_list  $clist
            wait_for_condition 10 100 {
                [$master get incr_key] eq [$slave get incr_key]
            } else {
                fail "incr_key in master:[$master get incr_key] and slave:[$slave get incr_key] are not \
                identical when clients were killed forcely."
            }
        }
    }
}

start_server {tags {"repl"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set num 10000000
        set load_handle0 [start_bg_complex_data $master_host $master_port 9 $num]
        # set load_handle1 [start_bg_complex_data $master_host $master_port 11 $num]
        # set load_handle2 [start_bg_complex_data $master_host $master_port 12 $num]
        after 1000

        test {slave sync with master during writing data} {
            $slave slaveof $master_host $master_port
            wait_for_condition 50 500 {
                [lindex [$slave role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$slave info replication]]
            } else {
                stop_bg_complex_data $load_handle0
                # stop_bg_complex_data $load_handle1
                # stop_bg_complex_data $load_handle2
                fail "slave can not sync with master during writing"
            }
            set keyslist [r -1 ssdbkeys *]

            puts "wait [llength $keyslist] keys identical....."
            foreach key $keyslist {
                wait_for_condition 50 100 {
                    [ r exists $key ] eq [ r -1 exists $key ]
                } else {
                    stop_bg_complex_data $load_handle0
                    fail "key:$key in master and slave not identical"
                }
            }
            stop_bg_complex_data $load_handle0
            # stop_bg_complex_data $load_handle1
            # stop_bg_complex_data $load_handle2
        }
    }
}

start_server {tags {"repl"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set num 100000
        set load_handle0 [start_bg_complex_data $master_host $master_port 9 $num]
        # set load_handle1 [start_bg_complex_data $master_host $master_port 11 $num]
        # set load_handle2 [start_bg_complex_data $master_host $master_port 12 $num]
        after 1000

        test {slave sync with master during writing data} {
            $slave slaveof $master_host $master_port
            wait_for_condition 50 500 {
                [lindex [$slave role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$slave info replication]]
            } else {
                stop_bg_complex_data $load_handle0
                # stop_bg_complex_data $load_handle1
                # stop_bg_complex_data $load_handle2
                fail "slave can not sync with master during writing"
            }
            stop_bg_complex_data $load_handle0
            # stop_bg_complex_data $load_handle1
            # stop_bg_complex_data $load_handle2
        }
    }
}

start_server {tags {"repl"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        set num 50000
        set load_handle0 [start_bg_complex_data $master_host $master_port 9 $num]
        set load_handle1 [start_bg_complex_data $master_host $master_port 11 $num]
        set load_handle2 [start_bg_complex_data $master_host $master_port 12 $num]

        test {First server should have role slave after SLAVEOF} {
            $slave slaveof $master_host $master_port
            after 1000
            s 0 role
        } {slave}

        test {Test replication with parallel clients writing in DB 0} {
            after 5000
            stop_bg_complex_data $load_handle0
            stop_bg_complex_data $load_handle1
            stop_bg_complex_data $load_handle2
            r config set maxmemory 0
            r -1 config set maxmemory 0
            compare_debug_digest
            # assert_equal [debug_digest r -1] [debug_digest r]
        }
    }
}

# not support config set min-slaves-*
#start_server {tags {"repl"}} {
#    start_server {} {
#        set master [srv -1 client]
#        set master_host [srv -1 host]
#        set master_port [srv -1 port]
#        set slave [srv 0 client]
#
#        test {First server should have role slave after SLAVEOF} {
#            $slave slaveof $master_host $master_port
#            wait_for_condition 50 100 {
#                [s 0 master_link_status] eq {up}
#            } else {
#                fail "Replication not started."
#            }
#        }
#        # slave->replstate not online when master link status is up
#        after 1000
#
#        test {With min-slaves-to-write (1,3): master should be writable} {
#            $master config set min-slaves-max-lag 3
#            $master config set min-slaves-to-write 1
#            $master set foo bar
#        } {OK}
#
#        test {With min-slaves-to-write (2,3): master should not be writable} {
#            $master config set min-slaves-max-lag 3
#            $master config set min-slaves-to-write 2
#            catch {$master set foo bar} e
#            set e
#        } {NOREPLICAS*}
#
#        test {With min-slaves-to-write: master not writable with lagged slave} {
#            $master config set min-slaves-max-lag 2
#            $master config set min-slaves-to-write 1
#            assert {[$master set foo bar] eq {OK}}
#            $slave deferred 1
#            $slave debug sleep 6
#            after 4000
#            catch {$master set foo bar} e
#            set e
#        } {NOREPLICAS*}
#    }
#}

start_server {tags {"repl"}} {
    start_server {} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]

        test {First server should have role slave after SLAVEOF} {
            $slave slaveof $master_host $master_port
            wait_for_condition 50 100 {
                [s 0 role] eq {slave}
            } else {
                fail "Replication not started."
            }

            # wait for sync complete.
            set retry 500
            while {$retry} {
                set info [r -1 info]
                if {[string match {*slave0:*state=online*} $info]} {
                    break
                } else {
                    incr retry -1
                    after 100
                }
            }
            if {$retry == 0} {
                error "assertion:Slaves not correctly synchronized"
            }
        }

        test {Replication of SPOP command -- alsoPropagate() API} {
            for {set n 0} {$n < 5} {incr n} {
                $master del myset
                set size [expr [randomInt 100]+1]
                set content {}
                for {set j 0} {$j < $size} {incr j} {
                    lappend content [randomValue]
                }
                $master sadd myset {*}$content

                # set count [expr [randomInt $size]]
                set count [expr [randomInt 100]]
                set result [$master spop myset $count]
                r -1 config set maxmemory 0
                r -1 dumpfromssdb myset
                wait_for_condition 10 50 {
                    [r -1 debug digest] == [r debug digest]
                } else {
                    fail "Digest not equal:master([r -1 debug digest]) and slave([r debug digest]) after too long time."
                }
                r -1 config set maxmemory 100M
            }
        }
    }
}
