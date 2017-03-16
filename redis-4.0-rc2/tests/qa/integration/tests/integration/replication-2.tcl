start_server {tags {"repl"}} {
    start_server {} {
        test {First server should have role slave after SLAVEOF} {
            r -1 slaveof [srv 0 host] [srv 0 port]
            after 1000
            s -1 role
        } {slave}

        test {If min-slaves-to-write is honored, write is accepted} {
            set retry 500
            while {$retry} {
                set info [r info]
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
            r config set min-slaves-to-write 1
            r config set min-slaves-max-lag 10
            r set foo 12345
            wait_for_condition 50 100 {
                [r -1 get foo] eq {12345}
            } else {
                fail "Write did not reached slave"
            }
        }

#        test {No write if min-slaves-to-write is < attached slaves} {
#            r config set min-slaves-to-write 2
#            r config set min-slaves-max-lag 10
#            catch {r set foo 12345} err
#            set err
#        } {NOREPLICAS*}

        test {If min-slaves-to-write is honored, write is accepted (again)} {
            r config set min-slaves-to-write 1
            r config set min-slaves-max-lag 10
            r set foo 12345
            wait_for_condition 50 100 {
                [r -1 get foo] eq {12345}
            } else {
                fail "Write did not reached slave"
            }
        }

#        test {No write if min-slaves-max-lag is > of the slave lag} {
#            r -1 deferred 1
#            r config set min-slaves-to-write 1
#            r config set min-slaves-max-lag 2
#            r -1 debug sleep 6
#            assert {[r set foo 12345] eq {OK}}
#            after 4000
#            catch {r set foo 12345} err
#            assert {[r -1 read] eq {OK}}
#            r -1 deferred 0
#            set err
#        } {NOREPLICAS*}

        test {min-slaves-to-write is ignored by slaves} {
            r config set min-slaves-to-write 1
            r config set min-slaves-max-lag 10
            r -1 config set min-slaves-to-write 1
            r -1 config set min-slaves-max-lag 10
            r set foo aaabbb
            wait_for_condition 100 100 {
                [r -1 get foo] eq {aaabbb}
            } else {
                fail "Write did not reached slave"
            }
        }

        # Fix parameters for the next test to work
        r config set min-slaves-to-write 0
        r -1 config set min-slaves-to-write 0
        r flushall
        after 1000

        test {MASTER and SLAVE dataset should be identical after complex ops} {
            set keyslist [createComplexDataset r 100000]

            r config set maxmemory 0
            r -1 config set maxmemory 0
            foreach key $keyslist {
                wait_for_condition 100 100 {
                    [ r exists $key ] eq [ r -1 exists $key ]
                } else {
                    fail "key:$key in master and slave not identical"
                }
            }
            assert_equal [debug_digest r] [debug_digest r -1]
            wait_for_condition 50 100 {
                [llength [sr keys *] ] eq 0
            } else {
                fail "master's ssdb keys should be null"
            }
            wait_for_condition 50 100 {
                [llength [sr -1 keys *] ] eq 0
            } else {
                fail "slave's ssdb keys should be null"
            }
        }
    }
}
