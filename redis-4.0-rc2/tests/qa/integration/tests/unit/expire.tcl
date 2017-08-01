start_server {tags {"expire"}} {
    test {#issue SWAP-68 for load policy optimize update bring in 20170612} {
        ssdbr set foo bar
        ssdbr pexpire foo 500
        after 1000
        ssdbr dbsize
    } {0}

    test {EXPIRE - set timeouts multiple times} {
        ssdbr set x foobar
        set v1 [ssdbr expire x 5]
        set v2 [ssdbr ttl x]
        set v3 [ssdbr expire x 10]
        set v4 [ssdbr ttl x]
        ssdbr expire x 2
        list $v1 $v2 $v3 $v4
    } {1 [45] 1 10}

    test {EXPIRE - It should be still possible to read 'x'} {
        ssdbr get x
    } {foobar}

    tags {"slow"} {
        test {EXPIRE - After 2.1 seconds the key should no longer be here} {
            after 2100
            list [ssdbr get x] [ssdbr exists x] [sr exists x]
        } {{} 0 0}
    }

    test {EXPIRE - negative time} {
        ssdbr del x
        ssdbr set x y
        ssdbr expire x -1

        wait_for_condition 10 1 {
            0 == [ssdbr exists x] &&
            0 == [sr exists x]
        } else {
            fail "key in redis or ssdb not be deleted!"
        }
    }

    test {EXPIRE - write on expire should work} {
        ssdbr del x
        ssdbr lpush x foo
        ssdbr expire x 1000
        ssdbr lpush x bar
        ssdbr lrange x 0 -1
    } {bar foo}

    test {EXPIREAT - Check for EXPIRE alike behavior} {
        ssdbr del x
        ssdbr set x foo
        ssdbr expireat x [expr [clock seconds]+15]
        ssdbr ttl x
    } {1[345]}

    test {SETEX - Set + Expire combo operation. Check for TTL} {
        ssdbr setex x 12 test
        ssdbr ttl x
    } {1[012]}

    test {SETEX - Check value} {
        ssdbr get x
    } {test}

    test {SETEX - Overwrite old key} {
        ssdbr setex y 1 foo
        ssdbr get y
    } {foo}

    tags {"slow"} {
        test {SETEX - Wait for the key to expire} {
            after 1100
            ssdbr get y
        } {}
    }

    test {SETEX - Wrong time parameter} {
        catch {r setex z -10 foo} e
        set _ $e
    } {*invalid expire*}

    test {PERSIST can undo an EXPIRE} {
        ssdbr set x foo
        ssdbr expire x 50
        list [ssdbr ttl x] [ssdbr persist x] [ssdbr ttl x] [ssdbr get x]
    } {50 1 -1 foo}

    test {PERSIST returns 0 against non existing or non volatile keys} {
        ssdbr set x foo
        list [ssdbr persist foo] [ssdbr persist nokeyatall]
    } {0 0}

    test {EXPIRE pricision is now the millisecond} {
        # This test is very likely to do a false positive if the
        # server is under pressure, so if it does not work give it a few more
        # chances.
        for {set j 0} {$j < 3} {incr j} {
            ssdbr del x
            ssdbr setex x 1 somevalue
            after 900
            set a [ssdbr get x]
            after 1100
            set b [ssdbr get x]
            if {$a eq {somevalue} && $b eq {}} break
        }
        list $a $b
    } {somevalue {}}

    test {PEXPIRE/PSETEX/PEXPIREAT can set sub-second expires} {
        # This test is very likely to do a false positive if the
        # server is under pressure, so if it does not work give it a few more
        # chances.
        for {set j 0} {$j < 3} {incr j} {
            ssdbr del x
            ssdbr del y
            ssdbr del z
            ssdbr psetex x 100 somevalue
            after 80
            set a [ssdbr get x]
            after 120
            set b [ssdbr get x]

            ssdbr set x somevalue
            ssdbr pexpire x 100
            after 80
            set c [ssdbr get x]
            after 120
            set d [ssdbr get x]

            ssdbr set x somevalue
            ssdbr pexpireat x [expr ([clock seconds]*1000)+100]
            after 80
            set e [ssdbr get x]
            after 120
            set f [ssdbr get x]

            if {$a eq {somevalue} && $b eq {} &&
                $c eq {somevalue} && $d eq {} &&
                $e eq {somevalue} && $f eq {}} break
        }
        list $a $b
    } {somevalue {}}

    test {TTL returns tiem to live in seconds} {
        ssdbr del x
        ssdbr setex x 10 somevalue
        set ttl [ssdbr ttl x]
        assert {$ttl > 8 && $ttl <= 10}
    }

    test {PTTL returns time to live in milliseconds} {
        ssdbr del x
        ssdbr setex x 1 somevalue
        set ttl [ssdbr pttl x]
        assert {$ttl > 900 && $ttl <= 1000}
    }

    test {TTL / PTTL return -1 if key has no expire} {
        ssdbr del x
        ssdbr set x hello
        list [ssdbr ttl x] [ssdbr pttl x]
    } {-1 -1}

    test {TTL / PTTL return -2 if key does not exit} {
        ssdbr del x
        list [ssdbr ttl x] [ssdbr pttl x]
    } {-2 -2}

    test {Redis should actively expire keys incrementally} {
        ssdbr flushdb
        ssdbr psetex key1 500 a
        ssdbr psetex key2 500 a
        ssdbr psetex key3 500 a
        set size1 [ssdbr dbsize]
        # Redis expires random keys ten times every second so we are
        # fairly sure that all the three keys should be evicted after
        # one second.
        after 1000
        set size2 [ssdbr dbsize]
        list $size1 $size2
    } {3 0}

    test {Redis should lazy expire keys} {
        ssdbr flushdb
        ssdbr debug set-active-expire 0
        ssdbr psetex key1 500 a
        ssdbr psetex key2 500 a
        ssdbr psetex key3 500 a
        set size1 [ssdbr dbsize]
        # Redis expires random keys ten times every second so we are
        # fairly sure that all the three keys should be evicted after
        # one second.
        after 1000
        set size2 [ssdbr dbsize]
        r get key1
        r get key2
        r get key3
        #TODO delete confirm
        after 10
        set size3 [ssdbr dbsize]
        ssdbr debug set-active-expire 1
        list $size1 $size2 $size3
    } {3 3 0}

    test {EXPIRE should not resurrect keys (issue #1026)} {
        ssdbr debug set-active-expire 0
        ssdbr set foo bar
        ssdbr pexpire foo 500
        after 1000
        ssdbr expire foo 10
        ssdbr debug set-active-expire 1
        ssdbr exists foo
    } {0}

    test {5 keys in, 5 keys out} {
        ssdbr flushdb
        wait_ssdb_reconnect
        ssdbr set a c
        ssdbr expire a 5
        ssdbr set t c
        ssdbr set e c
        ssdbr set s c
        ssdbr set foo b
        lsort [ssdbr keys *]
    } {a e foo s t}

    test {EXPIRE with empty string as TTL should report an error} {
        ssdbr set foo bar
        catch {r expire foo ""} e
        set e
    } {*not an integer*}
}
