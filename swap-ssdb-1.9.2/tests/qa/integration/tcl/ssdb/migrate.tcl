start_server {tags {"ssdb"}} {
    r flushdb
    set first [srv 0 client]
    set first_host [srv 0 host]
    set first_port [srv 0 port]

start_server {} {
    test {MIGRATE is able to migrate a key between two instances} {
        r set key "Some Value"
        $first flushdb
        assert {[r exists key] == 1}
        assert {[$first exists key] == 0}
        set ret [r migrate $first_host $first_port key 0 5000]
        assert {$ret eq {OK}}
        assert {[r exists key] == 0}
        assert {[$first exists key] == 1}
        assert {[$first get key] eq {Some Value}}
        if {$::expire} {
            assert {[$first ttl key] == -1}
        }
    }

    test {MIGRATE is able to copy a key between two instances} {
        r del list
        r lpush list a b c d
        $first flushdb

        assert {[r exists list] == 1}
        assert {[$first exists list] == 0}
        set ret [r migrate $first_host $first_port list 0 5000 copy]
        assert {$ret eq {OK}}
        assert {[r exists list] == 1}
        assert {[$first exists list] == 1}
        assert {[r lrange list 0 -1] eq [$first lrange list 0 -1]}
    }

    test {MIGRATE will not overwrite existing keys, unless REPLACE is used} {
        r del list
        $first del list
        r lpush list a b c d

        assert {[r exists list] == 1}
        assert {[$first exists list] == 0}
        $first set list somevalue
        catch {r migrate $first_host $first_port list 0 5000 copy} e
        assert_match {ERR*} $e
        assert {[r type list] == "list"}
        assert {[$first type list] == "string"}
        catch {r migrate $first_host $first_port list 0 5000} e
        assert_match {ERR*} $e
        assert {[r type list] == "list"}
        assert {[$first type list] == "string"}
        set res [r migrate $first_host $first_port list 0 5000 copy replace]
        assert {$ret eq {OK}}
        assert {[r exists list] == 1}
        assert {[$first exists list] == 1}
        assert {[r lrange list 0 -1] eq [$first lrange list 0 -1]}
    }

    if {$::expire} {
        test {MIGRATE propagates TTL correctly} {
            r set key "Some Value"
            $first flushdb

            assert {[r exists key] == 1}
            assert {[$first exists key] == 0}
            r expire key 10
            set ret [r migrate $first_host $first_port key 0 5000]
            assert {$ret eq {OK}}
            assert {[r exists key] == 0}
            assert {[$first exists key] == 1}
            assert {[$first get key] eq {Some Value}}
            assert {[$first ttl key] >= 7 && [$first ttl key] <= 10}
        }
    }

    test {MIGRATE can correctly transfer large values} {
        r del key
        $first flushdb
        for {set j 0} {$j < 40000} {incr j} {
            r rpush key 1 2 3 4 5 6 7 8 9 10
            r rpush key "item 1" "item 2" "item 3" "item 4" "item 5" \
                        "item 6" "item 7" "item 8" "item 9" "item 10"
        }
        assert {[string length [r dump key]] > (1024*64)}

        assert {[r exists key] == 1}
        assert {[$first exists key] == 0}
        set ret [r migrate $first_host $first_port key 0 10000]
        assert {$ret eq {OK}}
        assert {[r exists key] == 0}
        assert {[$first exists key] == 1}
        if {$::expire} {
            assert {[$first ttl key] == -1}
        }
        assert {[$first llen key] == 40000*20}
    }

    test {MIGRATE can correctly transfer hashes} {
        r del key
        $first flushdb
        r hmset key field1 "item 1" field2 "item 2" field3 "item 3" \
                    field4 "item 4" field5 "item 5" field6 "item 6"
        assert {[r exists key] == 1}
        assert {[$first exists key] == 0}
        set ret [r migrate $first_host $first_port key 0 10000]
        assert {$ret eq {OK}}
        assert {[r exists key] == 0}
        assert {[$first exists key] == 1}
        if {$::expire} {
            assert {[$first ttl key] == -1}
        }
    }

#    TODO debug sleep cannot work, so disable this case.
#    test {MIGRATE timeout actually works} {
#        r set key "Some Value"
#        $first flushdb
#
#        assert {[r exists key] == 1}
#        assert {[$first exists key] == 0}
#
#        set rd [redis $first_host $first_port 1]
#        $rd debug sleep 1.0 ; # Make ssdb_$first_port server unable to reply.
#        set e {}
#        catch {r migrate $first_host $first_port key 0 500} e
#        assert_match {IOERR*} $e
#    }
#
    test {MIGRATE can migrate multiple keys at once} {
        r set key1 "v1"
        r set key2 "v2"
        r set key3 "v3"
        $first flushdb

        assert {[r exists key1] == 1}
        assert {[$first exists key1] == 0}
        set ret [r migrate $first_host $first_port "" 0 5000 keys key1 key2 key3]
        assert {$ret eq {OK}}
        assert {[r exists key1] == 0}
        assert {[r exists key2] == 0}
        assert {[r exists key3] == 0}
        assert {[$first get key1] eq {v1}}
        assert {[$first get key2] eq {v2}}
        assert {[$first get key3] eq {v3}}
    }

    test {MIGRATE with multiple keys must have empty key arg} {
        catch {r MIGRATE 127.0.0.1 6379 NotEmpty 0 5000 keys a b c} e
        set e
    } {*empty string*}

    test {MIGRATE with mutliple keys migrate just existing ones} {
        r set key1 "v1"
        r set key2 "v2"
        r set key3 "v3"
        $first flushdb

        set ret [r migrate $first_host $first_port "" 0 5000 keys nokey-1 nokey-2 nokey-2]
        assert {$ret eq {NOKEY}}

        assert {[r exists key1] == 1}
        assert {[$first exists key1] == 0}
        set ret [r migrate $first_host $first_port "" 0 5000 keys nokey-1 key1 nokey-2 key2 nokey-3 key3]
        assert {$ret eq {OK}}
        assert {[r exists key1] == 0}
        assert {[r exists key2] == 0}
        assert {[r exists key3] == 0}
        assert {[$first get key1] eq {v1}}
        assert {[$first get key2] eq {v2}}
        assert {[$first get key3] eq {v3}}
    }

    test {MIGRATE with multiple keys: stress command rewriting} {
        r flushdb
        $first flushdb
        r mset a 1 b 2 c 3 d 4 c 5 e 6 f 7 g 8 h 9 i 10 l 11 m 12 n 13 o 14 p 15 q 16

            set ret [r migrate $first_host $first_port "" 0 5000 keys a b c d e f g h i l m n o p q]

            assert_equal 0 [r  del a b c d e f g h i l m n o p q]
            assert_equal 15 [$first  del a b c d e f g h i l m n o p q]
    }

    test {MIGRATE with multiple keys: delete just ack keys} {
        r flushdb
        $first flushdb
        r mset a 1 b 2 c 3 d 4 c 5 e 6 f 7 g 8 h 9 i 10 l 11 m 12 n 13 o 14 p 15 q 16

        $first mset c _ d _; # Two busy keys and no REPLACE used

        catch {r migrate $first_host $first_port "" 0 5000 keys a b c d e f g h i l m n o p q} e

        assert_equal 2 [r  del a b c d e f g h i l m n o p q]
        assert_equal 15 [$first  del a b c d e f g h i l m n o p q]
    }

    test {MIGRATE with wrong params} {
        r flushdb
        $first flushdb
        r set foo bar

        assert_error "ERR*connect*target*" {r migrate abc $first_port foo 0 5000}
        assert_error "IOERR*timeout*client*" {r migrate 111.111.111.111 $first_port foo 0 5000}
        # some different error info with redis
        puts "some different error info with redis!"
        assert_error "IOERR*timeout*client*" {r migrate $first_host abc foo 0 5000}
        # assert_error "IOERR*timeout*target*" {r migrate $first_host abc foo 0 5000}
        assert_error "IOERR*timeout*client*" {r migrate $first_host 1111 foo 0 5000}
        # assert_error "IOERR*timeout*target*" {r migrate $first_host 1111 foo 0 5000}
        assert_error "ERR*integer*out of range*" {r migrate $first_host $first_port foo a 5000}
        assert_error "ERR*integer*out of range*" {r migrate $first_host $first_port foo 0 abc}
        assert_error "ERR*syntax*" {r migrate $first_host $first_port foo 0 abc c}
        assert_error "ERR*syntax*" {r migrate $first_host $first_port foo 0 abc copy r}
        assert_error "ERR*syntax*" {r migrate $first_host $first_port foo 0 abc copy replace k}
        assert_error "ERR*KEYS*empty*" {r migrate $first_host $first_port foo 0 abc copy replace keys foo}
        assert_error "ERR*integer*out of range*" {r migrate $first_host $first_port "" 0 abc copy replace keys}
    }
    }
}
