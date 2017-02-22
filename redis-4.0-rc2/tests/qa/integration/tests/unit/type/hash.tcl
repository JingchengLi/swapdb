start_server {tags {"hash"}} {
    test "repeat hlen twice cause timeout #issue(happen when db select 9)" {
        ssdbr hset hash k v
        list [ssdbr hlen hash] [ssdbr hlen hash]
    } {1 1}

    test {HSET/HLEN - Small hash creation} {
        array set smallhash {}
        for {set i 0} {$i < 8} {incr i} {
            set key __avoid_collisions__[randstring 0 8 alpha]
            set val __avoid_collisions__[randstring 0 8 alpha]
            if {[info exists smallhash($key)]} {
                incr i -1
                continue
            }
            ssdbr hset smallhash $key $val
            set smallhash($key) $val
        }
        ssdbr hlen smallhash
    } {8}

    test {Is the small hash encoded with a ziplist?} {
        assert_encoding ziplist smallhash
    }

    test {HSET/HLEN - Big hash creation} {
        array set bighash {}
        for {set i 0} {$i < 1024} {incr i} {
            set key __avoid_collisions__[randstring 0 8 alpha]
            set val __avoid_collisions__[randstring 0 8 alpha]
            if {[info exists bighash($key)]} {
                incr i -1
                continue
            }
            ssdbr hset bighash $key $val
            set bighash($key) $val
        }
        list [ssdbr hlen bighash]
    } {1024}

    test {Is the big hash encoded with an hash table?} {
        assert_encoding hashtable bighash
    }

    test {HGET against the small hash} {
        set err {}
        foreach k [array names smallhash *] {
            if {$smallhash($k) ne [ssdbr hget smallhash $k]} {
                set err "$smallhash($k) != [ssdbr hget smallhash $k]"
                break
            }
        }
        set _ $err
    } {}

    test {HGET against the big hash} {
        set err {}
        foreach k [array names bighash *] {
            if {$bighash($k) ne [ssdbr hget bighash $k]} {
                set err "$bighash($k) != [ssdbr hget bighash $k]"
                break
            }
        }
        set _ $err
    } {}

    test {HGET against non existing key} {
        set rv {}
        lappend rv [ssdbr hget smallhash __123123123__]
        lappend rv [ssdbr hget bighash __123123123__]
        set _ $rv
    } {{} {}}

    test {HSET in update and insert mode} {
        set rv {}
        set k [lindex [array names smallhash *] 0]
        lappend rv [ssdbr hset smallhash $k newval1]
        set smallhash($k) newval1
        lappend rv [ssdbr hget smallhash $k]
        lappend rv [ssdbr hset smallhash __foobar123__ newval]
        set k [lindex [array names bighash *] 0]
        lappend rv [ssdbr hset bighash $k newval2]
        set bighash($k) newval2
        lappend rv [ssdbr hget bighash $k]
        lappend rv [ssdbr hset bighash __foobar123__ newval]
        lappend rv [ssdbr hdel smallhash __foobar123__]
        lappend rv [ssdbr hdel bighash __foobar123__]
        set _ $rv
    } {0 newval1 1 0 newval2 1 1 1}

    test {HSETNX target key missing - small hash} {
        ssdbr hsetnx smallhash __123123123__ foo
        ssdbr hget smallhash __123123123__
    } {foo}

    test {HSETNX target key exists - small hash} {
        ssdbr hsetnx smallhash __123123123__ bar
        set result [ssdbr hget smallhash __123123123__]
        ssdbr hdel smallhash __123123123__
        set _ $result
    } {foo}

    test {HSETNX target key missing - big hash} {
        ssdbr hsetnx bighash __123123123__ foo
        ssdbr hget bighash __123123123__
    } {foo}

    test {HSETNX target key exists - big hash} {
        ssdbr hsetnx bighash __123123123__ bar
        set result [ssdbr hget bighash __123123123__]
        ssdbr hdel bighash __123123123__
        set _ $result
    } {foo}

    test {HMSET wrong number of args} {
        catch {r hmset smallhash key1 val1 key2} err
        format $err
    } {*wrong number*}

    test {HMSET - small hash} {
        set args {}
        foreach {k v} [array get smallhash] {
            set newval [randstring 0 8 alpha]
            set smallhash($k) $newval
            lappend args $k $newval
        }
        ssdbr hmset smallhash {*}$args
    } {OK}

    test {HMSET - big hash} {
        set args {}
        foreach {k v} [array get bighash] {
            set newval [randstring 0 8 alpha]
            set bighash($k) $newval
            lappend args $k $newval
        }
        ssdbr hmset bighash {*}$args
    } {OK}

    test {HMGET against non existing key and fields} {
        set rv {}
        lappend rv [ssdbr hmget doesntexist __123123123__ __456456456__]
        lappend rv [ssdbr hmget smallhash __123123123__ __456456456__]
        lappend rv [ssdbr hmget bighash __123123123__ __456456456__]
        set _ $rv
    } {{{} {}} {{} {}} {{} {}}}

    test {HMGET against wrong type} {
        ssdbr set wrongtype somevalue
        assert_error "*ERR*" {r hmget wrongtype field1 field2}
    }

    test {HMGET - small hash} {
        set keys {}
        set vals {}
        foreach {k v} [array get smallhash] {
            lappend keys $k
            lappend vals $v
        }
        set err {}
        set result [ssdbr hmget smallhash {*}$keys]
        if {$vals ne $result} {
            set err "$vals != $result"
            break
        }
        set _ $err
    } {}

    test {HMGET - big hash} {
        set keys {}
        set vals {}
        foreach {k v} [array get bighash] {
            lappend keys $k
            lappend vals $v
        }
        set err {}
        set result [ssdbr hmget bighash {*}$keys]
        if {$vals ne $result} {
            set err "$vals != $result"
            break
        }
        set _ $err
    } {}

    test {HKEYS - small hash} {
        lsort [ssdbr hkeys smallhash]
    } [lsort [array names smallhash *]]

    test {HKEYS - big hash} {
        lsort [ssdbr hkeys bighash]
    } [lsort [array names bighash *]]

    test {HVALS - small hash} {
        set vals {}
        foreach {k v} [array get smallhash] {
            lappend vals $v
        }
        set _ [lsort $vals]
    } [lsort [ssdbr hvals smallhash]]

    test {HVALS - big hash} {
        set vals {}
        foreach {k v} [array get bighash] {
            lappend vals $v
        }
        set _ [lsort $vals]
    } [lsort [ssdbr hvals bighash]]

    test {HGETALL - small hash} {
        lsort [ssdbr hgetall smallhash]
    } [lsort [array get smallhash]]

    test {HGETALL - big hash} {
        lsort [ssdbr hgetall bighash]
    } [lsort [array get bighash]]

    test {HDEL and return value} {
        set rv {}
        lappend rv [ssdbr hdel smallhash nokey]
        lappend rv [ssdbr hdel bighash nokey]
        set k [lindex [array names smallhash *] 0]
        lappend rv [ssdbr hdel smallhash $k]
        lappend rv [ssdbr hdel smallhash $k]
        lappend rv [ssdbr hget smallhash $k]
        unset smallhash($k)
        set k [lindex [array names bighash *] 0]
        lappend rv [ssdbr hdel bighash $k]
        lappend rv [ssdbr hdel bighash $k]
        lappend rv [ssdbr hget bighash $k]
        unset bighash($k)
        set _ $rv
    } {0 0 1 0 {} 1 0 {}}

    test {HDEL - more than a single value} {
        set rv {}
        ssdbr del myhash
        ssdbr hmset myhash a 1 b 2 c 3
        assert_equal 0 [ssdbr hdel myhash x y]
        assert_equal 2 [ssdbr hdel myhash a c f]
        ssdbr hgetall myhash
    } {b 2}

    test {HDEL - hash becomes empty before deleting all specified fields} {
        ssdbr del myhash
        ssdbr hmset myhash a 1 b 2 c 3
        assert_equal 3 [ssdbr hdel myhash a b c d e]
        assert_equal 0 [ssdbr exists myhash]
    }

    test {HEXISTS} {
        set rv {}
        set k [lindex [array names smallhash *] 0]
        lappend rv [ssdbr hexists smallhash $k]
        lappend rv [ssdbr hexists smallhash nokey]
        set k [lindex [array names bighash *] 0]
        lappend rv [ssdbr hexists bighash $k]
        lappend rv [ssdbr hexists bighash nokey]
    } {1 0 1 0}

    test {Is a ziplist encoded Hash promoted on big payload?} {
        ssdbr hset smallhash foo [string repeat a 1024]
        ssdbr debug object smallhash
    } {*hashtable*}

    test {HINCRBY against non existing database key} {
        ssdbr del htest
        list [ssdbr hincrby htest foo 2]
    } {2}

    test {HINCRBY against non existing hash key} {
        set rv {}
        ssdbr hdel smallhash tmp
        ssdbr hdel bighash tmp
        lappend rv [ssdbr hincrby smallhash tmp 2]
        lappend rv [ssdbr hget smallhash tmp]
        lappend rv [ssdbr hincrby bighash tmp 2]
        lappend rv [ssdbr hget bighash tmp]
    } {2 2 2 2}

    test {HINCRBY against hash key created by hincrby itself} {
        set rv {}
        lappend rv [ssdbr hincrby smallhash tmp 3]
        lappend rv [ssdbr hget smallhash tmp]
        lappend rv [ssdbr hincrby bighash tmp 3]
        lappend rv [ssdbr hget bighash tmp]
    } {5 5 5 5}

    test {HINCRBY against hash key originally set with HSET} {
        ssdbr hset smallhash tmp 100
        ssdbr hset bighash tmp 100
        list [ssdbr hincrby smallhash tmp 2] [ssdbr hincrby bighash tmp 2]
    } {102 102}

    test {HINCRBY over 32bit value} {
        ssdbr hset smallhash tmp 17179869184
        ssdbr hset bighash tmp 17179869184
        list [ssdbr hincrby smallhash tmp 1] [ssdbr hincrby bighash tmp 1]
    } {17179869185 17179869185}

    test {HINCRBY over 32bit value with over 32bit increment} {
        ssdbr hset smallhash tmp 17179869184
        ssdbr hset bighash tmp 17179869184
        list [ssdbr hincrby smallhash tmp 17179869184] [ssdbr hincrby bighash tmp 17179869184]
    } {34359738368 34359738368}

    test {HINCRBY fails against hash value with spaces (left)} {
        ssdbr hset smallhash str " 11"
        ssdbr hset bighash str " 11"
        catch {r hincrby smallhash str 1} smallerr
        catch {r hincrby smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not an integer*" $smallerr]
        lappend rv [string match "ERR*not an integer*" $bigerr]
    } {1 1}

    test {HINCRBY fails against hash value with spaces (right)} {
        ssdbr hset smallhash str "11 "
        ssdbr hset bighash str "11 "
        catch {r hincrby smallhash str 1} smallerr
        catch {r hincrby smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not an integer*" $smallerr]
        lappend rv [string match "ERR*not an integer*" $bigerr]
    } {1 1}

    test {HINCRBY can detect overflows} {
        set e {}
        ssdbr hset hash n -9223372036854775484
        assert {[ssdbr hincrby hash n -1] == -9223372036854775485}
        catch {r hincrby hash n -10000} e
        set e
    } {*overflow*}

    test {HINCRBYFLOAT against non existing database key} {
        ssdbr del htest
        list [ssdbr hincrbyfloat htest foo 2.5]
    } {2.5}

    test {HINCRBYFLOAT against non existing hash key} {
        set rv {}
        ssdbr hdel smallhash tmp
        ssdbr hdel bighash tmp
        lappend rv [roundFloat [ssdbr hincrbyfloat smallhash tmp 2.5]]
        lappend rv [roundFloat [ssdbr hget smallhash tmp]]
        lappend rv [roundFloat [ssdbr hincrbyfloat bighash tmp 2.5]]
        lappend rv [roundFloat [ssdbr hget bighash tmp]]
    } {2.5 2.5 2.5 2.5}

    test {HINCRBYFLOAT against hash key created by hincrby itself} {
        set rv {}
        lappend rv [roundFloat [ssdbr hincrbyfloat smallhash tmp 3.5]]
        lappend rv [roundFloat [ssdbr hget smallhash tmp]]
        lappend rv [roundFloat [ssdbr hincrbyfloat bighash tmp 3.5]]
        lappend rv [roundFloat [ssdbr hget bighash tmp]]
    } {6 6 6 6}

    test {HINCRBYFLOAT against hash key originally set with HSET} {
        ssdbr hset smallhash tmp 100
        ssdbr hset bighash tmp 100
        list [roundFloat [ssdbr hincrbyfloat smallhash tmp 2.5]] \
             [roundFloat [ssdbr hincrbyfloat bighash tmp 2.5]]
    } {102.5 102.5}

    test {HINCRBYFLOAT over 32bit value} {
        ssdbr hset smallhash tmp 17179869184
        ssdbr hset bighash tmp 17179869184
        list [ssdbr hincrbyfloat smallhash tmp 1] \
             [ssdbr hincrbyfloat bighash tmp 1]
    } {17179869185 17179869185}

    test {HINCRBYFLOAT over 32bit value with over 32bit increment} {
        ssdbr hset smallhash tmp 17179869184
        ssdbr hset bighash tmp 17179869184
        list [ssdbr hincrbyfloat smallhash tmp 17179869184] \
             [ssdbr hincrbyfloat bighash tmp 17179869184]
    } {34359738368 34359738368}

    test {HINCRBYFLOAT fails against hash value with spaces (left)} {
        ssdbr hset smallhash str " 11"
        ssdbr hset bighash str " 11"
        catch {r hincrbyfloat smallhash str 1} smallerr
        catch {r hincrbyfloat smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not*float*" $smallerr]
        lappend rv [string match "ERR*not*float*" $bigerr]
    } {1 1}

    test {HINCRBYFLOAT fails against hash value with spaces (right)} {
        ssdbr hset smallhash str "11 "
        ssdbr hset bighash str "11 "
        catch {r hincrbyfloat smallhash str 1} smallerr
        catch {r hincrbyfloat smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not*float*" $smallerr]
        lappend rv [string match "ERR*not*float*" $bigerr]
    } {1 1}

   #    not support HSTRLEN
#    test {HSTRLEN against the small hash} {
#        set err {}
#        foreach k [array names smallhash *] {
#            if {[string length $smallhash($k)] ne [ssdbr hstrlen smallhash $k]} {
#                set err "[string length $smallhash($k)] != [ssdbr hstrlen smallhash $k]"
                break
            }
        }
        set _ $err
    } {}

    test {HSTRLEN against the big hash} {
        set err {}
        foreach k [array names bighash *] {
            if {[string length $bighash($k)] ne [ssdbr hstrlen bighash $k]} {
                set err "[string length $bighash($k)] != [ssdbr hstrlen bighash $k]"
                puts "HSTRLEN and logical length mismatch:"
                puts "key: $k"
                puts "Logical content: $bighash($k)"
                puts "Server  content: [ssdbr hget bighash $k]"
            }
        }
        set _ $err
    } {}

    test {HSTRLEN against non existing field} {
        set rv {}
        lappend rv [ssdbr hstrlen smallhash __123123123__]
        lappend rv [ssdbr hstrlen bighash __123123123__]
        set _ $rv
    } {0 0}

    test {HSTRLEN corner cases} {
        set vals {
            -9223372036854775808 9223372036854775807 9223372036854775808
            {} 0 -1 x
        }
        foreach v $vals {
            ssdbr hmset smallhash field $v
            ssdbr hmset bighash field $v
            set len1 [string length $v]
            set len2 [ssdbr hstrlen smallhash field]
            set len3 [ssdbr hstrlen bighash field]
            assert {$len1 == $len2}
            assert {$len2 == $len3}
        }
    }

    test {Hash ziplist regression test for large keys} {
        ssdbr hset hash kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk a
        ssdbr hset hash kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk b
        ssdbr hget hash kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk
    } {b}

    foreach size {10 512} {
        test "Hash fuzzing #1 - $size fields" {
            for {set times 0} {$times < 10} {incr times} {
                catch {unset hash}
                array set hash {}
                ssdbr del hash

                # Create
                for {set j 0} {$j < $size} {incr j} {
                    set field [randomValue]
                    set value [randomValue]
                    ssdbr hset hash $field $value
                    set hash($field) $value
                }

                # Verify
                foreach {k v} [array get hash] {
                    assert_equal $v [ssdbr hget hash $k]
                }
                assert_equal [array size hash] [ssdbr hlen hash]
            }
        }

        test "Hash fuzzing #2 - $size fields" {
            for {set times 0} {$times < 10} {incr times} {
                catch {unset hash}
                array set hash {}
                ssdbr del hash

                # Create
                for {set j 0} {$j < $size} {incr j} {
                    randpath {
                        set field [randomValue]
                        set value [randomValue]
                        ssdbr hset hash $field $value
                        set hash($field) $value
                    } {
                        set field [randomSignedInt 512]
                        set value [randomSignedInt 512]
                        ssdbr hset hash $field $value
                        set hash($field) $value
                    } {
                        randpath {
                            set field [randomValue]
                        } {
                            set field [randomSignedInt 512]
                        }
                        ssdbr hdel hash $field
                        unset -nocomplain hash($field)
                    }
                }

                # Verify
                foreach {k v} [array get hash] {
                    assert_equal $v [ssdbr hget hash $k]
                }
                assert_equal [array size hash] [ssdbr hlen hash]
            }
        }
    }

    test {Stress test the hash ziplist -> hashtable encoding conversion} {
        ssdbr config set hash-max-ziplist-entries 32
        for {set j 0} {$j < 100} {incr j} {
            ssdbr del myhash
            for {set i 0} {$i < 64} {incr i} {
                ssdbr hset myhash [randomValue] [randomValue]
            }
            assert {[ssdbr object encoding myhash] eq {hashtable}}
        }
    }

    # The following test can only be executed if we don't use Valgrind, and if
    # we are using x86_64 architecture, because:
    #
    # 1) Valgrind has floating point limitations, no support for 80 bits math.
    # 2) Other archs may have the same limits.
    #
    # 1.23 cannot be represented correctly with 64 bit doubles, so we skip
    # the test, since we are only testing pretty printing here and is not
    # a bug if the program outputs things like 1.299999...
    if {!$::valgrind || ![string match *x86_64* [exec uname -a]]} {
        test {Test HINCRBYFLOAT for correct float representation (issue #2846)} {
            ssdbr del myhash
            assert {[ssdbr hincrbyfloat myhash float 1.23] eq {1.23}}
            assert {[ssdbr hincrbyfloat myhash float 0.77] eq {2}}
            assert {[ssdbr hincrbyfloat myhash float -0.1] eq {1.9}}
        }
    }
}
