start_server {tags {"dump"}} {

    proc wait_key_exists_ssdb {key {level 0}} {
        wait_for_condition 50 10 {
            [sr $level exists $key] == 1
        } else {
            fail "$key not found in ssdb after some time."
        }
    }

    test {DUMP / RESTORE are able to serialize / unserialize a simple key} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        list [r exists foo] [r restore foo 0 $encoded] [r ttl foo] [r get foo]
    } {0 OK -1 bar}

    test {RESTORE can set an arbitrary expire to the materialized key} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        r restore foo 5000 $encoded
        set ttl [r pttl foo]
        assert {$ttl >= 3000 && $ttl <= 5000}
        r get foo
    } {bar}

    test {RESTORE can set an expire that overflows a 32 bit integer} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        r restore foo 2569591501 $encoded
        set ttl [r pttl foo]
        assert {$ttl >= (2569591501-3000) && $ttl <= 2569591501}
        r get foo
    } {bar}

    test {RESTORE returns an error of the key already exists} {
        r set foo bar
        set e {}
        catch {r restore foo 0 "..."} e
        set e
    } {*BUSYKEY*}

    test {RESTORE can overwrite an existing key with REPLACE} {
        r set foo bar1
        set encoded1 [r dump foo]
        r set foo bar2
        set encoded2 [r dump foo]
        r del foo
        r restore foo 0 $encoded1
        r restore foo 0 $encoded2 replace
        r get foo
    } {bar2}

    test {RESTORE can detect a syntax error for unrecongized options} {
        catch {r restore foo 0 "..." invalid-option} e
        set e
    } {*syntax*}

    test {DUMP of non existing key returns nil} {
        r dump nonexisting_key
    } {}

#    test {MIGRATE is caching connections} {
#        # Note, we run this as first test so that the connection cache
#        # is empty.
#        set first [srv 0 client]
#        r set key "Some Value"
#        start_server {tags {"repl"}} {
#            set second [srv 0 client]
#            set second_host [srv 0 host]
#            set second_port [srv 0 port]
#
#            assert_match {*migrate_cached_sockets:0*} [r -1 info]
#            r -1 migrate $second_host $second_port key 0 1000
#            assert_match {*migrate_cached_sockets:1*} [r -1 info]
#        }
#    }
#
#    test {MIGRATE cached connections are released after some time} {
#        after 15000
#        assert_match {*migrate_cached_sockets:0*} [r info]
#    }

    test {MIGRATE is able to migrate a key between two instances} {
        set first [srv 0 client]
        r set key "Some Value"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key] == 1}
            wait_for_dumpto_ssdb $first key
            assert {[$second exists key] == 0}
            set ret [r -1 migrate $second_host $second_port key 0 5000]
            assert {$ret eq {OK}}
            assert {[sr -1 exists key] == 0}
            assert {[$first exists key] == 0}
            wait_key_exists_ssdb key
            assert {[$second exists key] == 1}
            assert {[$second get key] eq {Some Value}}
            assert {[$second ttl key] == -1}
        }
    }

    test {MIGRATE is able to migrate a key between two instances multi times} {
        set first [srv 0 client]
        set first_host [srv 0 host]
        set first_port [srv 0 port]
        r set key "Some Value"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]
            for {set n 0} {$n < 100} {incr n} {
                r -1 migrate $second_host $second_port key 0 5000
                if {rand() < 0.5} {
                    wait_for_dumpto_ssdb $second key
                } else {
                    $second exists key ;# make it hot
                }
                r 0 migrate $first_host $first_port key 0 5000
                if {rand() < 0.5} {
                    wait_for_dumpto_ssdb $first key
                } else {
                    $first exists key ;# make it hot
                }
            }

            set ret [r -1 migrate $second_host $second_port key 0 5000]
            assert {$ret eq {OK}}
            assert {[sr -1 exists key] == 0}
            assert {[$first exists key] == 0}
            wait_key_exists_ssdb key
            assert {[$second exists key] == 1}
            assert {[$second get key] eq {Some Value}}
            assert {[$second ttl key] == -1}
        }
    }

    test {MIGRATE is able to copy a key between two instances} {
        set first [srv 0 client]
        r del list
        r lpush list a b c d
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists list] == 1}
            assert {[$second exists list] == 0}
            wait_for_dumpto_ssdb $first list
            set ret [r -1 migrate $second_host $second_port list 0 5000 copy]
            assert {$ret eq {OK}}
            assert {[sr -1 exists list] == 1}
            assert {[$first exists list] == 1}
            wait_key_exists_ssdb list
            assert {[$second exists list] == 1}
            assert {[$first lrange list 0 -1] eq [$second lrange list 0 -1]}
        }
    }

    test {MIGRATE will not overwrite existing keys, unless REPLACE is used} {
        set first [srv 0 client]
        r del list
        r lpush list a b c d
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists list] == 1}
            assert {[$second exists list] == 0}
            $second set list somevalue
            wait_for_dumpto_ssdb $first list
            catch {r -1 migrate $second_host $second_port list 0 5000} e
            assert_match {ERR*} $e
            assert {[$first exists list] == 1}
            assert_equal "somevalue" [sr get list]
            wait_for_dumpto_ssdb $first list
            catch {r -1 migrate $second_host $second_port list 0 5000 copy} e
            assert_match {ERR*} $e
            assert {[$first exists list] == 1}
            assert_equal "somevalue" [sr get list]
            wait_for_dumpto_ssdb $first list
            set ret [r -1 migrate $second_host $second_port list 0 5000 copy replace]
            assert {$ret eq {OK}}
            assert {[sr -1 exists list] == 1}
            assert {[$first exists list] == 1}
            wait_key_exists_ssdb list
            assert {[$second exists list] == 1}
            assert {[$first lrange list 0 -1] eq [$second lrange list 0 -1]}
        }
    }

    test {MIGRATE propagates TTL correctly} {
        set first [srv 0 client]
        r set key "Some Value"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key] == 1}
            assert {[$second exists key] == 0}
            $first expire key 10
            wait_for_dumpto_ssdb $first key
            set ret [r -1 migrate $second_host $second_port key 0 5000]
            assert {$ret eq {OK}}
            assert {[sr -1 exists key] == 0}
            assert {[$first exists key] == 0}
            wait_key_exists_ssdb key
            assert {[$second exists key] == 1}
            assert {[$second get key] eq {Some Value}}
            assert {[$second ttl key] >= 7 && [$second ttl key] <= 10}
        }
    }

    foreach {loop time} {40 100 400 100 4000 1000 20000 5000} {
        test "MIGRATE can correctly transfer large values($loop)" {
            set first [srv 0 client]
            r config set maxmemory 0
            r del key
            for {set j 0} {$j < $loop} {incr j} {
                r rpush key 1 2 3 4 5 6 7 8 9 10
                r rpush key "item 1" "item 2" "item 3" "item 4" "item 5" \
                "item 6" "item 7" "item 8" "item 9" "item 10"
            }
            # assert {[string length [r dump key]] > (1024*64)}
            r config set maxmemory 100M
            start_server {tags {"repl"}} {
                set second [srv 0 client]
                set second_host [srv 0 host]
                set second_port [srv 0 port]

                assert {[$first exists key] == 1}
                assert {[$second exists key] == 0}
                dumpto_ssdb_and_wait $first key
                set pretime [clock milliseconds]
                set ret [r -1 migrate $second_host $second_port key 0 $time]
                puts "$loop migrate cost [expr [clock milliseconds]-$pretime] ms"
                assert {$ret eq {OK}}
                assert {[sr -1 exists key] == 0}
                assert {[$first exists key] == 0}
                wait_key_exists_ssdb key
                assert {[$second exists key] == 1}
                assert {[$second ttl key] == -1}
                assert {[$second llen key] == $loop*20}
            }
        }
    }

    test {MIGRATE can correctly transfer hashes} {
        set first [srv 0 client]
        r del key
        r hmset key field1 "item 1" field2 "item 2" field3 "item 3" \
                    field4 "item 4" field5 "item 5" field6 "item 6"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key] == 1}
            assert {[$second exists key] == 0}
            wait_for_dumpto_ssdb $first key
            set ret [r -1 migrate $second_host $second_port key 0 10000]
            assert {$ret eq {OK}}
            assert {[$first exists key] == 0}
            assert {[$second exists key] == 1}
            assert {[$second ttl key] == -1}
        }
    }

    test {MIGRATE timeout actually works} {
        set first [srv 0 client]
        r set key "Some Value"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key] == 1}
            assert {[$second exists key] == 0}

            set rd [redis_deferring_client]
            $rd debug sleep 1.0 ; # Make second server unable to reply.
            set e {}
            wait_for_dumpto_ssdb $first key
            catch {r -1 migrate $second_host $second_port key 0 500} e
            assert_match {IOERR*} $e
        }
    }

    test {MIGRATE with wrong params} {
        set first [srv 0 client]
        $first flushdb
        $first set foo bar
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            wait_for_dumpto_ssdb $first foo

            assert_error "*ERR*" {$first migrate abc $second_port foo 0 5000}
            assert_error "IOERR*timeout*client*" {$first migrate 111.111.111.111 8889 foo 0 5000}
            assert_error "IOERR*timeout*target*" {$first migrate $second_host abc foo 0 5000}
            assert_error "IOERR*timeout*target*" {$first migrate $second_host 1111 foo 0 5000}
            assert_error "ERR*integer*out of range*" {$first migrate $second_host $second_port foo a 5000}
            assert_error "ERR*integer*out of range*" {$first migrate $second_host $second_port foo 0 abc}
            assert_error "ERR*syntax*" {$first migrate $second_host $second_port foo 0 abc c}
            assert_error "ERR*syntax*" {$first migrate $second_host $second_port foo 0 abc copy r}
            assert_error "ERR*syntax*" {$first migrate $second_host $second_port foo 0 abc copy replace k}
            assert_error "ERR*Not*jdjr-mode*" {$first migrate $second_host $second_port foo 0 abc copy replace keys foo}
            assert_error "ERR*Not*jdjr-mode*" {$first migrate $second_host $second_port "" 0 abc copy replace keys}
        }
    }

    test {MIGRATE can migrate multiple keys in loop} {
        set first [srv 0 client]
        r set key1 "v1"
        r set key2 "v2"
        r set key3 "v3"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key1] == 1}
            assert {[$second exists key1] == 0}
            foreach key {key1 key2 key3} {
                set ret [r -1 migrate $second_host $second_port $key 0 5000]
                assert {$ret eq {OK}}
            }
            assert {[$first exists key1] == 0}
            assert {[$first exists key2] == 0}
            assert {[$first exists key3] == 0}
            assert {[$second get key1] eq {v1}}
            assert {[$second get key2] eq {v2}}
            assert {[$second get key3] eq {v3}}
        }
    }

#    test {MIGRATE with multiple keys must have empty key arg} {
#        catch {r MIGRATE 127.0.0.1 6379 NotEmpty 0 5000 keys a b c} e
#        set e
#    } {*empty string*}
#
#    test {MIGRATE with mutliple keys migrate just existing ones} {
#        set first [srv 0 client]
#        r set key1 "v1"
#        r set key2 "v2"
#        r set key3 "v3"
#        start_server {tags {"repl"}} {
#            set second [srv 0 client]
#            set second_host [srv 0 host]
#            set second_port [srv 0 port]
#
#            set ret [r -1 migrate $second_host $second_port "" 0 5000 keys nokey-1 nokey-2 nokey-2]
#            assert {$ret eq {NOKEY}}
#
#            assert {[$first exists key1] == 1}
#            assert {[$second exists key1] == 0}
#            set ret [r -1 migrate $second_host $second_port "" 0 5000 keys nokey-1 key1 nokey-2 key2 nokey-3 key3]
#            assert {$ret eq {OK}}
#            assert {[$first exists key1] == 0}
#            assert {[$first exists key2] == 0}
#            assert {[$first exists key3] == 0}
#            assert {[$second get key1] eq {v1}}
#            assert {[$second get key2] eq {v2}}
#            assert {[$second get key3] eq {v3}}
#        }
#    }
#
#    test {MIGRATE with multiple keys: stress command rewriting} {
#        set first [srv 0 client]
#        r flushdb
#        r mset a 1 b 2 c 3 d 4 c 5 e 6 f 7 g 8 h 9 i 10 l 11 m 12 n 13 o 14 p 15 q 16
#        start_server {tags {"repl"}} {
#            set second [srv 0 client]
#            set second_host [srv 0 host]
#            set second_port [srv 0 port]
#
#            set ret [r -1 migrate $second_host $second_port "" 0 5000 keys a b c d e f g h i l m n o p q]
#
#            assert {[$first dbsize] == 0}
#            assert {[$second dbsize] == 15}
#        }
#    }
#
#    test {MIGRATE with multiple keys: delete just ack keys} {
#        set first [srv 0 client]
#        r flushdb
#        r mset a 1 b 2 c 3 d 4 c 5 e 6 f 7 g 8 h 9 i 10 l 11 m 12 n 13 o 14 p 15 q 16
#        start_server {tags {"repl"}} {
#            set second [srv 0 client]
#            set second_host [srv 0 host]
#            set second_port [srv 0 port]
#
#            $second mset c _ d _; # Two busy keys and no REPLACE used
#
#            catch {r -1 migrate $second_host $second_port "" 0 5000 keys a b c d e f g h i l m n o p q} e
#
#            assert {[$first dbsize] == 2}
#            assert {[$second dbsize] == 15}
#            assert {[$first exists c] == 1}
#            assert {[$first exists d] == 1}
#        }
#    }
}
