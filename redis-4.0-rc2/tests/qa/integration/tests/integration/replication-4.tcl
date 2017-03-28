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
            r -1 select 16
            set keyslist [r -1 keys *]
            r -1 select 0

            foreach key $keyslist {
                wait_for_condition 100 100 {
                    [ r exists $key ] eq [ r -1 exists $key ]
                } else {
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
            assert_equal [debug_digest r -1] [debug_digest r]
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
            $master del myset
            set size [expr 1+[randomInt 100]]
            set content {}
            for {set j 0} {$j < $size} {incr j} {
                lappend content [randomValue]
            }
            $master sadd myset {*}$content

            set count [randomInt 100]
            set result [$master spop myset $count]

           assert_equal [debug_digest r -1] [debug_digest r] "SPOP replication inconsistency"
        }
    }
}
