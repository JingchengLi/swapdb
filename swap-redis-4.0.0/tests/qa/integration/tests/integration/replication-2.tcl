start_server {tags {"repl"}} {
    set master [srv 0 client]
    start_server {} {
        set slave [srv 0 client]
        test {First server should have role slave after SLAVEOF} {
            $slave slaveof [srv -1 host] [srv -1 port]
            after 1000
            s role
        } {slave}

        test {If min-slaves-to-write is honored, write is accepted} {
            wait_for_online $master
            $master config set min-slaves-to-write 1
            $master config set min-slaves-max-lag 10
            $master set foo 12345
            wait_for_condition 50 100 {
                [$slave get foo] eq {12345}
            } else {
                fail "Write did not reached slave"
            }
        }

#        test {No write if min-slaves-to-write is < attached slaves} {
#            $master config set min-slaves-to-write 2
#            $master config set min-slaves-max-lag 10
#            catch {$master set foo 12345} err
#            set err
#        } {NOREPLICAS*}

        test {If min-slaves-to-write is honored, write is accepted (again)} {
            $master config set min-slaves-to-write 1
            $master config set min-slaves-max-lag 10
            $master set foo 12345
            wait_for_condition 50 100 {
                [$slave get foo] eq {12345}
            } else {
                fail "Write did not reached slave"
            }
        }

#        test {No write if min-slaves-max-lag is > of the slave lag} {
#            $slave deferred 1
#            $master config set min-slaves-to-write 1
#            $master config set min-slaves-max-lag 2
#            $slave debug sleep 6
#            assert {[$master set foo 12345] eq {OK}}
#            after 4000
#            catch {$master set foo 12345} err
#            assert {[$slave read] eq {OK}}
#            $slave deferred 0
#            set err
#        } {NOREPLICAS*}

        test {min-slaves-to-write is ignored by slaves} {
            $master config set min-slaves-to-write 1
            $master config set min-slaves-max-lag 10
            $slave config set min-slaves-to-write 1
            $slave config set min-slaves-max-lag 10
            $master set foo aaabbb
            wait_for_condition 100 100 {
                [$slave get foo] eq {aaabbb}
            } else {
                fail "Write did not reached slave"
            }
        }

        # Fix parameters for the next test to work
        $master config set min-slaves-to-write 0
        $slave config set min-slaves-to-write 0
        $master flushall
        after 1000

        test {MASTER and SLAVE dataset should be identical after complex ops} {
            if {$::accurate} {
                set keyslist [createComplexDataset $master 100000]
            } else {
                set keyslist [createComplexDataset $master 1000]
            }

            $master config set maxmemory 0
            $slave config set maxmemory 0
            foreach key $keyslist {
                wait_for_condition 30 100 {
                    [ $master exists $key ] eq [ $slave exists $key ]
                } else {
                    fail "key:$key in master and slave not identical"
                }
            }
            compare_debug_digest
            wait_for_condition 50 100 {
                [llength [sr -1 keys *] ] eq 0
            } else {
                fail "master's ssdb keys should be null"
            }
            wait_for_condition 50 100 {
                [llength [sr keys *] ] eq 0
            } else {
                fail "slave's ssdb keys should be null"
            }
        }
    }
}
