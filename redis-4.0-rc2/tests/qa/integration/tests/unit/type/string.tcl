start_server {tags {"string"}} {
    test {SET and GET an item} {
        ssdbr set x foobar
        ssdbr get x
    } {foobar}

    test {SET and GET an empty item} {
        ssdbr set x {}
        ssdbr get x
    } {}

    test {Very big payload in GET/SET} {
        set buf [string repeat "abcd" 1000000]
        ssdbr set foo $buf
        ssdbr get foo
    } [string repeat "abcd" 1000000]

    tags {"slow"} {
        test {Very big payload random access} {
            set err {}
            array set payload {}
            for {set j 0} {$j < 100} {incr j} {
                set size [expr 1+[randomInt 100000]]
                set buf [string repeat "pl-$j" $size]
                set payload($j) $buf
                ssdbr set bigpayload_$j $buf
            }
            for {set j 0} {$j < 1000} {incr j} {
                set index [randomInt 100]
                set buf [ssdbr get bigpayload_$index]
                if {$buf != $payload($index)} {
                    set err "Values differ: I set '$payload($index)' but I read back '$buf'"
                    break
                }
            }
            unset payload
            set _ $err
        } {}

        test {SET 10000 numeric keys and access all them in reverse order} {
            ssdbr flushdb
            set err {}
            for {set x 0} {$x < 10000} {incr x} {
                ssdbr set $x $x
            }
            set sum 0
            for {set x 9999} {$x >= 0} {incr x -1} {
                set val [ssdbr get $x]
                if {$val ne $x} {
                    set err "Element at position $x is $val instead of $x"
                    break
                }
            }
            set _ $err
        } {}

        test {DBSIZE should be 10000 now} {
            ssdbr dbsize
        } {10000}
    }

    test "SETNX target key missing" {
        ssdbr del novar
        assert_equal 1 [ssdbr setnx novar foobared]
        assert_equal "foobared" [ssdbr get novar]
    }

    test "SETNX target key exists" {
        ssdbr set novar foobared
        assert_equal 0 [ssdbr setnx novar blabla]
        assert_equal "foobared" [ssdbr get novar]
    }

    test "SETNX against not-expired volatile key" {
        ssdbr set x 10
        ssdbr expire x 10000
        assert_equal 0 [ssdbr setnx x 20]
        assert_equal 10 [ssdbr get x]
    }

    test "SETNX against expired volatile key" {
        # Make it very unlikely for the key this test uses to be expired by the
        # active expiry cycle. This is tightly coupled to the implementation of
        # active expiry and dbAdd() but currently the only way to test that
        # SETNX expires a key when it should have been.
        for {set x 0} {$x < 9999} {incr x} {
            ssdbr setex key-$x 3600 value
        }

        # This will be one of 10000 expiring keys. A cycle is executed every
        # 100ms, sampling 10 keys for being expired or not.  This key will be
        # expired for at most 1s when we wait 2s, resulting in a total sample
        # of 100 keys. The probability of the success of this test being a
        # false positive is therefore approx. 1%.
        ssdbr set x 10
        ssdbr expire x 1

        # Wait for the key to expire
        after 2000

        assert_equal 1 [ssdbr setnx x 20]
        assert_equal 20 [ssdbr get x]
    }

    #    No support multi_keys
#    test {MGET} {
#        ssdbr flushdb
#        ssdbr set foo BAR
#        ssdbr set bar FOO
#        ssdbr mget foo bar
#    } {BAR FOO}
#
#    test {MGET against non existing key} {
#        ssdbr mget foo baazz bar
#    } {BAR {} FOO}
#
#    test {MGET against non-string key} {
#        ssdbr sadd myset ciao
#        ssdbr sadd myset bau
#        ssdbr mget foo baazz bar myset
#    } {BAR {} FOO {}}

    test {GETSET (set new value)} {
        ssdbr del foo
        list [ssdbr getset foo xyz] [ssdbr get foo]
    } {{} xyz}

    test {GETSET (replace old value)} {
        ssdbr set foo bar
        list [ssdbr getset foo xyz] [ssdbr get foo]
    } {bar xyz}

    #    No support multi_keys
 #   test {MSET base case} {
 #       ssdbr mset x 10 y "foo bar" z "x x x x x x x\n\n\r\n"
 #       ssdbr mget x y z
 #   } [list 10 {foo bar} "x x x x x x x\n\n\r\n"]
 #
 #   test {MSET wrong number of args} {
 #       catch {r mset x 10 y "foo bar" z} err
 #       format $err
 #   } {*wrong number*}
 #
 #   test {MSETNX with already existent key} {
 #       list [ssdbr msetnx x1 xxx y2 yyy x 20] [ssdbr exists x1] [ssdbr exists y2]
 #   } {0 0 0}
 #
 #   test {MSETNX with not existing keys} {
 #       list [ssdbr msetnx x1 xxx y2 yyy] [ssdbr get x1] [ssdbr get y2]
 #   } {1 xxx yyy}

    test "STRLEN against non-existing key" {
        assert_equal 0 [ssdbr strlen notakey]
    }

    test "STRLEN against integer-encoded value" {
        ssdbr set myinteger -555
        assert_equal 4 [ssdbr strlen myinteger]
    }

    test "STRLEN against plain string" {
        ssdbr set mystring "foozzz0123456789 baz"
        assert_equal 20 [ssdbr strlen mystring]
    }

    test "SETBIT against non-existing key" {
        ssdbr del mykey
        assert_equal 0 [ssdbr setbit mykey 1 1]
        assert_equal [binary format B* 01000000] [ssdbr get mykey]
    }

    test "SETBIT against string-encoded key" {
        # Ascii "@" is integer 64 = 01 00 00 00
        ssdbr set mykey "@"

        assert_equal 0 [ssdbr setbit mykey 2 1]
        assert_equal [binary format B* 01100000] [ssdbr get mykey]
        assert_equal 1 [ssdbr setbit mykey 1 0]
        assert_equal [binary format B* 00100000] [ssdbr get mykey]
    }

    test "SETBIT against integer-encoded key" {
        # Ascii "1" is integer 49 = 00 11 00 01
        ssdbr set mykey 1
        assert_encoding int mykey

        assert_equal 0 [ssdbr setbit mykey 6 1]
        assert_equal [binary format B* 00110011] [ssdbr get mykey]
        assert_equal 1 [ssdbr setbit mykey 2 0]
        assert_equal [binary format B* 00010011] [ssdbr get mykey]
    }

    test "SETBIT against key with wrong type" {
        ssdbr del mykey
        ssdbr lpush mykey "foo"
        assert_error "WRONGTYPE*" {r setbit mykey 0 1}
    }

    test "SETBIT with out of range bit offset" {
        ssdbr del mykey
        assert_error "*out of range*" {r setbit mykey [expr 4*1024*1024*1024] 1}
        assert_error "*out of range*" {r setbit mykey -1 1}
    }

    test "SETBIT with non-bit argument" {
        ssdbr del mykey
        assert_error "*out of range*" {r setbit mykey 0 -1}
        assert_error "*out of range*" {r setbit mykey 0  2}
        assert_error "*out of range*" {r setbit mykey 0 10}
        assert_error "*out of range*" {r setbit mykey 0 20}
    }

    test "SETBIT fuzzing" {
        set str ""
        set len [expr 256*8]
        ssdbr del mykey

        for {set i 0} {$i < 2000} {incr i} {
            set bitnum [randomInt $len]
            set bitval [randomInt 2]
            set fmt [format "%%-%ds%%d%%-s" $bitnum]
            set head [string range $str 0 $bitnum-1]
            set tail [string range $str $bitnum+1 end]
            set str [string map {" " 0} [format $fmt $head $bitval $tail]]

            ssdbr setbit mykey $bitnum $bitval
            assert_equal [binary format B* $str] [ssdbr get mykey]
        }
    }

    test "GETBIT against non-existing key" {
        ssdbr del mykey
        assert_equal 0 [ssdbr getbit mykey 0]
    }

    test "GETBIT against string-encoded key" {
        # Single byte with 2nd and 3rd bit set
        ssdbr set mykey "`"

        # In-range
        assert_equal 0 [ssdbr getbit mykey 0]
        assert_equal 1 [ssdbr getbit mykey 1]
        assert_equal 1 [ssdbr getbit mykey 2]
        assert_equal 0 [ssdbr getbit mykey 3]

        # Out-range
        assert_equal 0 [ssdbr getbit mykey 8]
        assert_equal 0 [ssdbr getbit mykey 100]
        assert_equal 0 [ssdbr getbit mykey 10000]
    }

    test "GETBIT against integer-encoded key" {
        ssdbr set mykey 1
        assert_encoding int mykey

        # Ascii "1" is integer 49 = 00 11 00 01
        assert_equal 0 [ssdbr getbit mykey 0]
        assert_equal 0 [ssdbr getbit mykey 1]
        assert_equal 1 [ssdbr getbit mykey 2]
        assert_equal 1 [ssdbr getbit mykey 3]

        # Out-range
        assert_equal 0 [ssdbr getbit mykey 8]
        assert_equal 0 [ssdbr getbit mykey 100]
        assert_equal 0 [ssdbr getbit mykey 10000]
    }

    test "SETRANGE against non-existing key" {
        ssdbr del mykey
        assert_equal 3 [ssdbr setrange mykey 0 foo]
        assert_equal "foo" [ssdbr get mykey]

        ssdbr del mykey
        assert_equal 0 [ssdbr setrange mykey 0 ""]
        assert_equal 0 [ssdbr exists mykey]

        ssdbr del mykey
        assert_equal 4 [ssdbr setrange mykey 1 foo]
        assert_equal "\000foo" [ssdbr get mykey]
    }

    test "SETRANGE against string-encoded key" {
        ssdbr set mykey "foo"
        assert_equal 3 [ssdbr setrange mykey 0 b]
        assert_equal "boo" [ssdbr get mykey]

        ssdbr set mykey "foo"
        assert_equal 3 [ssdbr setrange mykey 0 ""]
        assert_equal "foo" [ssdbr get mykey]

        ssdbr set mykey "foo"
        assert_equal 3 [ssdbr setrange mykey 1 b]
        assert_equal "fbo" [ssdbr get mykey]

        ssdbr set mykey "foo"
        assert_equal 7 [ssdbr setrange mykey 4 bar]
        assert_equal "foo\000bar" [ssdbr get mykey]
    }

    test "SETRANGE against integer-encoded key" {
        ssdbr set mykey 1234
        assert_encoding int mykey
        assert_equal 4 [r setrange mykey 0 2]
        assert_encoding raw mykey
        assert_equal 2234 [ssdbr get mykey]

        # Shouldn't change encoding when nothing is set
        ssdbr set mykey 1234
        assert_encoding int mykey
        assert_equal 4 [r setrange mykey 0 ""]
        assert_encoding int mykey
        assert_equal 1234 [ssdbr get mykey]

        ssdbr set mykey 1234
        assert_encoding int mykey
        assert_equal 4 [r setrange mykey 1 3]
        assert_encoding raw mykey
        assert_equal 1334 [ssdbr get mykey]

        ssdbr set mykey 1234
        assert_encoding int mykey
        assert_equal 6 [r setrange mykey 5 2]
        assert_encoding raw mykey
        assert_equal "1234\0002" [ssdbr get mykey]
    }

    test "SETRANGE against key with wrong type" {
        ssdbr del mykey
        ssdbr lpush mykey "foo"
        assert_error "WRONGTYPE*" {r setrange mykey 0 bar}
    }

    test "SETRANGE with out of range offset" {
        ssdbr del mykey
        assert_error "*maximum allowed size*" {r setrange mykey [expr 512*1024*1024-4] world}

        ssdbr set mykey "hello"
        assert_error "*out of range*" {r setrange mykey -1 world}
        assert_error "*maximum allowed size*" {r setrange mykey [expr 512*1024*1024-4] world}
    }

    test "GETRANGE against non-existing key" {
        ssdbr del mykey
        assert_equal "" [ssdbr getrange mykey 0 -1]
    }

    test "GETRANGE against string value" {
        ssdbr set mykey "Hello World"
        assert_equal "Hell" [ssdbr getrange mykey 0 3]
        assert_equal "Hello World" [ssdbr getrange mykey 0 -1]
        assert_equal "orld" [ssdbr getrange mykey -4 -1]
        assert_equal "" [ssdbr getrange mykey 5 3]
        assert_equal " World" [ssdbr getrange mykey 5 5000]
        assert_equal "Hello World" [ssdbr getrange mykey -5000 10000]
    }

    test "GETRANGE against integer-encoded value" {
        ssdbr set mykey 1234
        assert_equal "123" [ssdbr getrange mykey 0 2]
        assert_equal "1234" [ssdbr getrange mykey 0 -1]
        assert_equal "234" [ssdbr getrange mykey -3 -1]
        assert_equal "" [ssdbr getrange mykey 5 3]
        assert_equal "4" [ssdbr getrange mykey 3 5000]
        assert_equal "1234" [ssdbr getrange mykey -5000 10000]
    }

    test "GETRANGE fuzzing" {
        for {set i 0} {$i < 1000} {incr i} {
            ssdbr set bin [set bin [randstring 0 1024 binary]]
            set _start [set start [randomInt 1500]]
            set _end [set end [randomInt 1500]]
            if {$_start < 0} {set _start "end-[abs($_start)-1]"}
            if {$_end < 0} {set _end "end-[abs($_end)-1]"}
            assert_equal [string range $bin $_start $_end] [ssdbr getrange bin $start $end]
        }
    }

    test {Extended SET can detect syntax errors} {
        set e {}
        catch {r set foo bar non-existing-option} e
        set e
    } {*syntax*}

    test {Extended SET NX option} {
        ssdbr del foo
        set v1 [ssdbr set foo 1 nx]
        set v2 [ssdbr set foo 2 nx]
        list $v1 $v2 [ssdbr get foo]
    } {OK {} 1}

    test {Extended SET XX option} {
        ssdbr del foo
        set v1 [ssdbr set foo 1 xx]
        ssdbr set foo bar
        set v2 [ssdbr set foo 2 xx]
        list $v1 $v2 [ssdbr get foo]
    } {{} OK 2}

    test {Extended SET EX option} {
        ssdbr del foo
        ssdbr set foo bar ex 10
        set ttl [ssdbr ttl foo]
        assert {$ttl <= 10 && $ttl > 5}
    }

    test {Extended SET PX option} {
        ssdbr del foo
        ssdbr set foo bar px 10000
        set ttl [ssdbr ttl foo]
        assert {$ttl <= 10 && $ttl > 5}
    }

    test {Extended SET using multiple options at once} {
        ssdbr set foo val
        assert {[ssdbr set foo bar xx px 10000] eq {OK}}
        set ttl [ssdbr ttl foo]
        assert {$ttl <= 10 && $ttl > 5}
    }

    test {GETRANGE with huge ranges, Github issue #1844} {
        ssdbr set foo bar
        ssdbr getrange foo 0 4294967297
    } {bar}
}
