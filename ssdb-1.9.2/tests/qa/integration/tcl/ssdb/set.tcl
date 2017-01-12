source "./ssdb/init_ssdb.tcl"
start_server {
    tags {"ssdb"}
    overrides {
        "set-max-intset-entries" 512
    }
} {
    proc create_set {key entries} {
        r del $key
        foreach entry $entries { r sadd $key $entry }
    }

    test {SADD, SCARD, SISMEMBER, SMEMBERS basics - regular set} {
        create_set myset {foo}
        assert_encoding hashtable myset
        assert_equal 1 [r sadd myset bar]
        assert_equal 0 [r sadd myset bar]
        assert_equal 2 [r scard myset]
        assert_equal 1 [r sismember myset foo]
        assert_equal 1 [r sismember myset bar]
        assert_equal 0 [r sismember myset bla]
        assert_equal {bar foo} [lsort [r smembers myset]]
    }

    test {SADD, SCARD, SISMEMBER, SMEMBERS basics - intset} {
        create_set myset {17}
        assert_encoding intset myset
        assert_equal 1 [r sadd myset 16]
        assert_equal 0 [r sadd myset 16]
        assert_equal 2 [r scard myset]
        assert_equal 1 [r sismember myset 16]
        assert_equal 1 [r sismember myset 17]
        assert_equal 0 [r sismember myset 18]
        assert_equal {16 17} [lsort [r smembers myset]]
    }

    test {SADD against non set} {
        r lpush mylist foo
        assert_error ERR* {r sadd mylist bar}
    }

    test "SADD a non-integer against an intset" {
        create_set myset {1 2 3}
        assert_encoding intset myset
        assert_equal 1 [r sadd myset a]
        assert_encoding hashtable myset
    }

    test "SADD an integer larger than 64 bits" {
        create_set myset {213244124402402314402033402}
        assert_encoding hashtable myset
        assert_equal 1 [r sismember myset 213244124402402314402033402]
    }

    test "SADD overflows the maximum allowed integers in an intset" {
        r del myset
        for {set i 0} {$i < 512} {incr i} { r sadd myset $i }
        assert_encoding intset myset
        assert_equal 1 [r sadd myset 512]
        assert_encoding hashtable myset
    }

    test {Variadic SADD} {
        r del myset
        assert_equal 3 [r sadd myset a b c]
        assert_equal 2 [r sadd myset A a b c B]
        assert_equal [lsort {A a b c B}] [lsort [r smembers myset]]
    }

    test {SREM basics - regular set} {
        create_set myset {foo bar ciao}
        assert_encoding hashtable myset
        assert_equal 0 [r srem myset qux]
        assert_equal 1 [r srem myset foo]
        assert_equal {bar ciao} [lsort [r smembers myset]]
    }

    test {SREM basics - intset} {
        create_set myset {3 4 5}
        assert_encoding intset myset
        assert_equal 0 [r srem myset 6]
        assert_equal 1 [r srem myset 4]
        assert_equal {3 5} [lsort [r smembers myset]]
    }

    test {SREM with multiple arguments} {
        r del myset
        r sadd myset a b c d
        assert_equal 0 [r srem myset k k k]
        assert_equal 2 [r srem myset b d x y]
        lsort [r smembers myset]
    } {a c}

    test {SREM variadic version with more args needed to destroy the key} {
        r del myset
        r sadd myset 1 2 3
        r srem myset 1 2 3 4 5 6 7 8
    } {3}

    foreach {type} {hashtable} {
        for {set i 1} {$i <= 5} {incr i} {
            r del [format "set%d" $i]
        }
        for {set i 0} {$i < 200} {incr i} {
            r sadd set1 $i
            r sadd set2 [expr $i+195]
        }
        foreach i {199 195 1000 2000} {
            r sadd set3 $i
        }
        for {set i 5} {$i < 200} {incr i} {
            r sadd set4 $i
        }
        r sadd set5 0

        # To make sure the sets are encoded as the type we are testing -- also
        # when the VM is enabled and the values may be swapped in and out
        # while the tests are running -- an extra element is added to every
        # set that determines its encoding.
        set large 200
        if {$type eq "hashtable"} {
            set large foo
        }

        for {set i 1} {$i <= 5} {incr i} {
            r sadd [format "set%d" $i] $large
        }

        test "Generated sets must be encoded as $type" {
            for {set i 1} {$i <= 5} {incr i} {
                assert_encoding $type [format "set%d" $i]
            }
        }

#        test "SINTER with two sets - $type" {
#            assert_equal [list 195 196 197 198 199 $large] [lsort [r sinter set1 set2]]
#        }
#
#        test "SINTERSTORE with two sets - $type" {
#            r sinterstore setres set1 set2
#            assert_encoding $type setres
#            assert_equal [list 195 196 197 198 199 $large] [lsort [r smembers setres]]
#        }
#
#        test "SINTERSTORE with two sets, after a DEBUG RELOAD - $type" {
#            r debug reload
#            r sinterstore setres set1 set2
#            assert_encoding $type setres
#            assert_equal [list 195 196 197 198 199 $large] [lsort [r smembers setres]]
#        }

        test "SUNION with two sets - $type" {
            set expected [lsort -uniq "[r smembers set1] [r smembers set2]"]
            assert_equal $expected [lsort [r sunion set1 set2]]
        }

        test "SUNIONSTORE with two sets - $type" {
            r sunionstore setres set1 set2
            assert_encoding $type setres
            set expected [lsort -uniq "[r smembers set1] [r smembers set2]"]
            assert_equal $expected [lsort [r smembers setres]]
        }

#        test "SINTER against three sets - $type" {
#            assert_equal [list 195 199 $large] [lsort [r sinter set1 set2 set3]]
#        }
#
#        test "SINTERSTORE with three sets - $type" {
#            r sinterstore setres set1 set2 set3
#            assert_equal [list 195 199 $large] [lsort [r smembers setres]]
#        }

        test "SUNION with non existing keys - $type" {
            set expected [lsort -uniq "[r smembers set1] [r smembers set2]"]
            assert_equal $expected [lsort [r sunion nokey1 set1 set2 nokey2]]
        }

#        test "SDIFF with two sets - $type" {
#            assert_equal {0 1 2 3 4} [lsort [r sdiff set1 set4]]
#        }
#
#        test "SDIFF with three sets - $type" {
#            assert_equal {1 2 3 4} [lsort [r sdiff set1 set4 set5]]
#        }
#
#        test "SDIFFSTORE with three sets - $type" {
#            r sdiffstore setres set1 set4 set5
#            # When we start with intsets, we should always end with intsets.
#            if {$type eq {intset}} {
#                assert_encoding intset setres
#            }
#            assert_equal {1 2 3 4} [lsort [r smembers setres]]
#        }
    }

    test "SUNION against non-set should throw error" {
        r set key1 x
        assert_error "ERR*" {r sunion key1 noset}
    }

    test "SUNIONSTORE against non existing keys should delete dstkey" {
        r del setres
        r set setres xxx
        assert_equal 0 [r sunionstore setres foo111 bar222]
        assert_equal 0 [r exists setres]
    }

    tags {slow} {
        test {intsets implementation stress testing} {
            for {set j 0} {$j < 20} {incr j} {
                unset -nocomplain s
                array set s {}
                r del s
                set len [randomInt 1024]
                for {set i 0} {$i < $len} {incr i} {
                    randpath {
                        set data [randomInt 65536]
                    } {
                        set data [randomInt 4294967296]
                    } {
                        set data [randomInt 18446744073709551616]
                    }
                    set s($data) $i
                    r sadd s $data
                }

                assert_equal [lsort [r smembers s]] [lsort [array names s]]
                foreach i [array names s] {
                    r srem s $i
                    array unset s $i
                }

                assert_equal [r scard s] 0
                assert_equal [array size s] 0
            }
        }
    }
}
