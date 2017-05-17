start_server {tags {"repl"}} {
    start_server {} {
        test {MASTER and SLAVE consistency with single key expire} {
            r -1 slaveof [srv 0 host] [srv 0 port]
            wait_for_condition 50 100 {
                [s -1 master_link_status] eq {up}
            } else {
                fail "Replication not started."
            }
            r set foo bar
            r expire foo 1
            after 2000 ;# Make sure everything expired before taking the digest

            set oldmaxmemory [lindex [ r config get maxmemory ] 1]
            wait_for_condition 10 100 {
                [ r exists foo ] == 0 &&
                [ r -1 exists foo ] == 0
            } else {
                fail "key in master and slave not identical"
            }
        }
    }
}

start_server {tags {"repl"}} {
    start_server {} {
        test {First server should have role slave after SLAVEOF} {
            r -1 slaveof [srv 0 host] [srv 0 port]
            wait_for_condition 50 100 {
                [s -1 master_link_status] eq {up}
            } else {
                fail "Replication not started."
            }
        }

        if {$::accurate} {set numops 5000} else {set numops 500}

        test {MASTER and SLAVE consistency with expire} {
            set keyslist [ createComplexDataset r $numops useexpire ]
            after 4000 ;# Make sure everything expired before taking the digest

            set oldmaxmemory [lindex [ r config get maxmemory ] 1]
            r config set maxmemory 0 ;# load all keys to redis
            foreach key $keyslist {
                wait_for_condition 50 100 {
                    [ r exists $key ] eq [ r -1 exists $key ]
                } else {
                    fail "key:$key in master and slave not identical [ r -1 exists $key ] [ r exists $key ]"
                }
            }
            # r keys *   ;# Force DEL syntesizing to slave
            after 1000 ;# Wait another second. Now everything should be fine.
            # if {[r debug digest] ne [r -1 debug digest]} {
                # set csv1 [csvdump r]
                # set csv2 [csvdump {r -1}]
                # set fd [open /tmp/repldump1.txt w]
                # puts -nonewline $fd $csv1
                # close $fd
                # set fd [open /tmp/repldump2.txt w]
                # puts -nonewline $fd $csv2
                # close $fd
                # puts "Master - Slave inconsistency"
                # puts "Run diff -u against /tmp/repldump*.txt for more info"
            # }
            assert_equal [r debug digest] [r -1 debug digest]
            r config set maxmemory $oldmaxmemory
        }
    }
}

# not support eval
#start_server {tags {"repl"}} {
#    start_server {} {
#        test {First server should have role slave after SLAVEOF} {
#            r -1 slaveof [srv 0 host] [srv 0 port]
#            wait_for_condition 50 100 {
#                [s -1 master_link_status] eq {up}
#            } else {
#                fail "Replication not started."
#            }
#        }
#
#        set numops 20000 ;# Enough to trigger the Script Cache LRU eviction.
#
#        # While we are at it, enable AOF to test it will be consistent as well
#        # after the test.
#        r config set appendonly yes
#
#        test {MASTER and SLAVE consistency with EVALSHA replication} {
#            array set oldsha {}
#            for {set j 0} {$j < $numops} {incr j} {
#                set key "key:$j"
#                # Make sure to create scripts that have different SHA1s
#                set script "return redis.call('incr','$key')"
#                set sha1 [r eval "return redis.sha1hex(\"$script\")" 0]
#                set oldsha($j) $sha1
#                r eval $script 0
#                set res [r evalsha $sha1 0]
#                assert {$res == 2}
#                # Additionally call one of the old scripts as well, at random.
#                set res [r evalsha $oldsha([randomInt $j]) 0]
#                assert {$res > 2}
#
#                # Trigger an AOF rewrite while we are half-way, this also
#                # forces the flush of the script cache, and we will cover
#                # more code as a result.
#                if {$j == $numops / 2} {
#                    catch {r bgrewriteaof}
#                }
#            }
#
#            wait_for_condition 50 100 {
#                [r dbsize] == $numops &&
#                [r -1 dbsize] == $numops &&
#                [r debug digest] eq [r -1 debug digest]
#            } else {
#                set csv1 [csvdump r]
#                set csv2 [csvdump {r -1}]
#                set fd [open /tmp/repldump1.txt w]
#                puts -nonewline $fd $csv1
#                close $fd
#                set fd [open /tmp/repldump2.txt w]
#                puts -nonewline $fd $csv2
#                close $fd
#                puts "Master - Slave inconsistency"
#                puts "Run diff -u against /tmp/repldump*.txt for more info"
#
#            }
#
#            set old_digest [r debug digest]
#            r config set appendonly no
#            r debug loadaof
#            set new_digest [r debug digest]
#            assert {$old_digest eq $new_digest}
#        }
#    }
#}
