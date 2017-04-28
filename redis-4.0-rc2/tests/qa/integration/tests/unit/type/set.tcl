start_server {
    tags {"type"}
    overrides {
        maxmemory 0
        "set-max-intset-entries" 512
    }
} {
    proc create_set {key entries} {
        ssdbr del $key
        foreach entry $entries { r sadd $key $entry }
    }

    test {SADD, SCARD, SISMEMBER, SMEMBERS basics - regular set} {
        create_set myset {foo}
        assert_encoding hashtable myset
        assert_equal 1 [ssdbr sadd myset bar]
        assert_equal 0 [ssdbr sadd myset bar]
        assert_equal 2 [ssdbr scard myset]
        assert_equal 1 [ssdbr sismember myset foo]
        assert_equal 1 [ssdbr sismember myset bar]
        assert_equal 0 [ssdbr sismember myset bla]
        assert_equal {bar foo} [lsort [ssdbr smembers myset]]
    }

    test {SADD, SCARD, SISMEMBER, SMEMBERS basics - intset} {
        create_set myset {17}
        assert_encoding intset myset
        assert_equal 1 [ssdbr sadd myset 16]
        assert_equal 0 [ssdbr sadd myset 16]
        assert_equal 2 [ssdbr scard myset]
        assert_equal 1 [ssdbr sismember myset 16]
        assert_equal 1 [ssdbr sismember myset 17]
        assert_equal 0 [ssdbr sismember myset 18]
        assert_equal {16 17} [lsort [ssdbr smembers myset]]
    }

    test {SADD against non set} {
        ssdbr lpush mylist foo
        assert_error WRONGTYPE* {r sadd mylist bar}
    }

    test "SADD a non-integer against an intset" {
        create_set myset {1 2 3}
        assert_encoding intset myset
        assert_equal 1 [ssdbr sadd myset a]
        assert_encoding hashtable myset
    }

    test "SADD an integer larger than 64 bits" {
        create_set myset {213244124402402314402033402}
        assert_encoding hashtable myset
        assert_equal 1 [ssdbr sismember myset 213244124402402314402033402]
    }

    test "SADD overflows the maximum allowed integers in an intset" {
        ssdbr del myset
        for {set i 0} {$i < 512} {incr i} { r sadd myset $i }
        assert_encoding intset myset
        assert_equal 1 [ssdbr sadd myset 512]
        assert_encoding hashtable myset
    }

    test {Variadic SADD} {
        ssdbr del myset
        assert_equal 3 [ssdbr sadd myset a b c]
        assert_equal 2 [ssdbr sadd myset A a b c B]
        assert_equal [lsort {A a b c B}] [lsort [ssdbr smembers myset]]
    }

    test "Set encoding after DEBUG RELOAD" {
        ssdbr del myintset myhashset mylargeintset
        for {set i 0} {$i <  100} {incr i} { r sadd myintset $i }
        for {set i 0} {$i < 1280} {incr i} { r sadd mylargeintset $i }
        for {set i 0} {$i <  256} {incr i} { r sadd myhashset [format "i%03d" $i] }
        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset

        ssdbr debug reload
        assert_encoding intset myintset
        assert_encoding hashtable mylargeintset
        assert_encoding hashtable myhashset
    }

    test {SREM basics - regular set} {
        create_set myset {foo bar ciao}
        assert_encoding hashtable myset
        assert_equal 0 [ssdbr srem myset qux]
        assert_equal 1 [ssdbr srem myset foo]
        assert_equal {bar ciao} [lsort [ssdbr smembers myset]]
    }

    test {SREM basics - intset} {
        create_set myset {3 4 5}
        assert_encoding intset myset
        assert_equal 0 [ssdbr srem myset 6]
        assert_equal 1 [ssdbr srem myset 4]
        assert_equal {3 5} [lsort [ssdbr smembers myset]]
    }

    test {SREM with multiple arguments} {
        ssdbr del myset
        ssdbr sadd myset a b c d
        assert_equal 0 [ssdbr srem myset k k k]
        assert_equal 2 [ssdbr srem myset b d x y]
        lsort [ssdbr smembers myset]
    } {a c}

    test {SREM variadic version with more args needed to destroy the key} {
        ssdbr del myset
        ssdbr sadd myset 1 2 3
        ssdbr srem myset 1 2 3 4 5 6 7 8
    } {3}

#    foreach {type} {hashtable intset} {
#        for {set i 1} {$i <= 5} {incr i} {
#            ssdbr del [format "set%d" $i]
#        }
#        for {set i 0} {$i < 200} {incr i} {
#            ssdbr sadd set1 $i
#            ssdbr sadd set2 [expr $i+195]
#        }
#        foreach i {199 195 1000 2000} {
#            ssdbr sadd set3 $i
#        }
#        for {set i 5} {$i < 200} {incr i} {
#            ssdbr sadd set4 $i
#        }
#        ssdbr sadd set5 0
#
#        # To make sure the sets are encoded as the type we are testing -- also
#        # when the VM is enabled and the values may be swapped in and out
#        # while the tests are running -- an extra element is added to every
#        # set that determines its encoding.
#        set large 200
#        if {$type eq "hashtable"} {
#            set large foo
#        }
#
#        for {set i 1} {$i <= 5} {incr i} {
#            ssdbr sadd [format "set%d" $i] $large
#        }
#
#        test "Generated sets must be encoded as $type" {
#            for {set i 1} {$i <= 5} {incr i} {
#                assert_encoding $type [format "set%d" $i]
#            }
#        }
#
#        test "SINTER with two sets - $type" {
#            assert_equal [list 195 196 197 198 199 $large] [lsort [ssdbr sinter set1 set2]]
#        }
#
#        test "SINTERSTORE with two sets - $type" {
#            ssdbr sinterstore setres set1 set2
#            assert_encoding $type setres
#            assert_equal [list 195 196 197 198 199 $large] [lsort [ssdbr smembers setres]]
#        }
#
#        test "SINTERSTORE with two sets, after a DEBUG RELOAD - $type" {
#            ssdbr debug reload
#            ssdbr sinterstore setres set1 set2
#            assert_encoding $type setres
#            assert_equal [list 195 196 197 198 199 $large] [lsort [ssdbr smembers setres]]
#        }
#
#        test "SUNION with two sets - $type" {
#            set expected [lsort -uniq "[ssdbr smembers set1] [ssdbr smembers set2]"]
#            assert_equal $expected [lsort [ssdbr sunion set1 set2]]
#        }
#
#        test "SUNIONSTORE with two sets - $type" {
#            ssdbr sunionstore setres set1 set2
#            assert_encoding $type setres
#            set expected [lsort -uniq "[ssdbr smembers set1] [ssdbr smembers set2]"]
#            assert_equal $expected [lsort [ssdbr smembers setres]]
#        }
#
#        test "SINTER against three sets - $type" {
#            assert_equal [list 195 199 $large] [lsort [ssdbr sinter set1 set2 set3]]
#        }
#
#        test "SINTERSTORE with three sets - $type" {
#            ssdbr sinterstore setres set1 set2 set3
#            assert_equal [list 195 199 $large] [lsort [ssdbr smembers setres]]
#        }
#
#        test "SUNION with non existing keys - $type" {
#            set expected [lsort -uniq "[ssdbr smembers set1] [ssdbr smembers set2]"]
#            assert_equal $expected [lsort [ssdbr sunion nokey1 set1 set2 nokey2]]
#        }
#
#        test "SDIFF with two sets - $type" {
#            assert_equal {0 1 2 3 4} [lsort [ssdbr sdiff set1 set4]]
#        }
#
#        test "SDIFF with three sets - $type" {
#            assert_equal {1 2 3 4} [lsort [ssdbr sdiff set1 set4 set5]]
#        }
#
#        test "SDIFFSTORE with three sets - $type" {
#            ssdbr sdiffstore setres set1 set4 set5
#            # When we start with intsets, we should always end with intsets.
#            if {$type eq {intset}} {
#                assert_encoding intset setres
#            }
#            assert_equal {1 2 3 4} [lsort [ssdbr smembers setres]]
#        }
#    }

#    test "SDIFF with first set empty" {
#        ssdbr del set1 set2 set3
#        ssdbr sadd set2 1 2 3 4
#        ssdbr sadd set3 a b c d
#        ssdbr sdiff set1 set2 set3
#    } {}
#
#    test "SDIFF with same set two times" {
#        ssdbr del set1
#        ssdbr sadd set1 a b c 1 2 3 4 5 6
#        ssdbr sdiff set1 set1
#    } {}
#
#    test "SDIFF fuzzing" {
#        for {set j 0} {$j < 100} {incr j} {
#            unset -nocomplain s
#            array set s {}
#            set args {}
#            set num_sets [expr {[randomInt 10]+1}]
#            for {set i 0} {$i < $num_sets} {incr i} {
#                set num_elements [randomInt 100]
#                ssdbr del set_$i
#                lappend args set_$i
#                while {$num_elements} {
#                    set ele [randomValue]
#                    ssdbr sadd set_$i $ele
#                    if {$i == 0} {
#                        set s($ele) x
#                    } else {
#                        unset -nocomplain s($ele)
#                    }
#                    incr num_elements -1
#                }
#            }
#            set result [lsort [ssdbr sdiff {*}$args]]
#            assert_equal $result [lsort [array names s]]
#        }
#    }
#
#    test "SINTER against non-set should throw error" {
#        ssdbr set key1 x
#        assert_error "WRONGTYPE*" {r sinter key1 noset}
#    }
#
#    test "SUNION against non-set should throw error" {
#        ssdbr set key1 x
#        assert_error "WRONGTYPE*" {r sunion key1 noset}
#    }
#
#    test "SINTER should handle non existing key as empty" {
#        ssdbr del set1 set2 set3
#        ssdbr sadd set1 a b c
#        ssdbr sadd set2 b c d
#        ssdbr sinter set1 set2 set3
#    } {}
#
#    test "SINTER with same integer elements but different encoding" {
#        ssdbr del set1 set2
#        ssdbr sadd set1 1 2 3
#        ssdbr sadd set2 1 2 3 a
#        ssdbr srem set2 a
#        assert_encoding intset set1
#        assert_encoding hashtable set2
#        lsort [ssdbr sinter set1 set2]
#    } {1 2 3}
#
#    test "SINTERSTORE against non existing keys should delete dstkey" {
#        ssdbr set setres xxx
#        assert_equal 0 [ssdbr sinterstore setres foo111 bar222]
#        assert_equal 0 [ssdbr exists setres]
#    }
#
#    test "SUNIONSTORE against non existing keys should delete dstkey" {
#        ssdbr set setres xxx
#        assert_equal 0 [ssdbr sunionstore setres foo111 bar222]
#        assert_equal 0 [ssdbr exists setres]
#    }

    foreach {type contents} {hashtable {a b c} intset {1 2 3}} {
        test "SPOP basics - $type" {
            create_set myset $contents
            assert_encoding $type myset
            assert_equal $contents [lsort [list [ssdbr spop myset] [ssdbr spop myset] [ssdbr spop myset]]]
            assert_equal 0 [ssdbr scard myset]
        }

        test "SPOP with <count>=1 - $type" {
            create_set myset $contents
            assert_encoding $type myset
            assert_equal $contents [lsort [list [ssdbr spop myset 1] [ssdbr spop myset 1] [ssdbr spop myset 1]]]
            assert_equal 0 [ssdbr scard myset]
        }

        test "SRANDMEMBER - $type" {
            create_set myset $contents
            unset -nocomplain myset
            array set myset {}
            for {set i 0} {$i < 100} {incr i} {
                set myset([ssdbr srandmember myset]) 1
            }
            assert_equal $contents [lsort [array names myset]]
        }
    }

    foreach {type contents} {
        hashtable {a b c d e f g h i j k l m n o p q r s t u v w x y z} 
        intset {1 10 11 12 13 14 15 16 17 18 19 2 20 21 22 23 24 25 26 3 4 5 6 7 8 9}
    } {
        test "SPOP with <count>" {
            create_set myset $contents
            assert_encoding $type myset
            assert_equal $contents [lsort [concat [ssdbr spop myset 11] [ssdbr spop myset 9] [ssdbr spop myset 0] [ssdbr spop myset 4] [ssdbr spop myset 1] [ssdbr spop myset 0] [ssdbr spop myset 1] [ssdbr spop myset 0]]]
            assert_equal 0 [ssdbr scard myset]
        }
    }

    # As seen in intsetRandomMembers
    test "SPOP using integers, testing Knuth's and Floyd's algorithm" {
        create_set myset {1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20}
        assert_encoding intset myset
        assert_equal 20 [ssdbr scard myset]
        ssdbr spop myset 1
        assert_equal 19 [ssdbr scard myset]
        ssdbr spop myset 2
        assert_equal 17 [ssdbr scard myset]
        ssdbr spop myset 3
        assert_equal 14 [ssdbr scard myset]
        ssdbr spop myset 10
        assert_equal 4 [ssdbr scard myset]
        ssdbr spop myset 10
        assert_equal 0 [ssdbr scard myset]
        ssdbr spop myset 1
        assert_equal 0 [ssdbr scard myset]
    } {}

    test "SPOP using integers with Knuth's algorithm" {
        ssdbr spop nonexisting_key 100
    } {}

    test "SPOP new implementation: code path #1" {
        set content {1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20}
        create_set myset $content
        set res [ssdbr spop myset 30]
        assert {[lsort $content] eq [lsort $res]}
    }

    test "SPOP new implementation: code path #2" {
        set content {1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20}
        create_set myset $content
        set res [ssdbr spop myset 2]
        assert {[llength $res] == 2}
        assert {[ssdbr scard myset] == 18}
        set union [concat [ssdbr smembers myset] $res]
        assert {[lsort $union] eq [lsort $content]}
    }

    test "SPOP new implementation: code path #3" {
        set content {1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20}
        create_set myset $content
        set res [ssdbr spop myset 18]
        assert {[llength $res] == 18}
        assert {[ssdbr scard myset] == 2}
        set union [concat [ssdbr smembers myset] $res]
        assert {[lsort $union] eq [lsort $content]}
    }

    test "SRANDMEMBER with <count> against non existing key" {
        ssdbr srandmember nonexisting_key 100
    } {}

    foreach {type contents} {
        hashtable {
            1 5 10 50 125 50000 33959417 4775547 65434162
            12098459 427716 483706 2726473884 72615637475
            MARY PATRICIA LINDA BARBARA ELIZABETH JENNIFER MARIA
            SUSAN MARGARET DOROTHY LISA NANCY KAREN BETTY HELEN
            SANDRA DONNA CAROL RUTH SHARON MICHELLE LAURA SARAH
            KIMBERLY DEBORAH JESSICA SHIRLEY CYNTHIA ANGELA MELISSA
            BRENDA AMY ANNA REBECCA VIRGINIA KATHLEEN
        }
        intset {
            0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
            20 21 22 23 24 25 26 27 28 29
            30 31 32 33 34 35 36 37 38 39
            40 41 42 43 44 45 46 47 48 49
        }
    } {
        test "SRANDMEMBER with <count> - $type" {
            create_set myset $contents
            unset -nocomplain myset
            array set myset {}
            foreach ele [ssdbr smembers myset] {
                set myset($ele) 1
            }
            assert_equal [lsort $contents] [lsort [array names myset]]

            # Make sure that a count of 0 is handled correctly.
            assert_equal [ssdbr srandmember myset 0] {}

            # We'll stress different parts of the code, see the implementation
            # of SRANDMEMBER for more information, but basically there are
            # four different code paths.
            #
            # PATH 1: Use negative count.
            #
            # 1) Check that it returns repeated elements.
            set res [ssdbr srandmember myset -100]
            assert_equal [llength $res] 100

            # 2) Check that all the elements actually belong to the
            # original set.
            foreach ele $res {
                assert {[info exists myset($ele)]}
            }

            # 3) Check that eventually all the elements are returned.
            unset -nocomplain auxset
            set iterations 1000
            while {$iterations != 0} {
                incr iterations -1
                set res [ssdbr srandmember myset -10]
                foreach ele $res {
                    set auxset($ele) 1
                }
                if {[lsort [array names myset]] eq
                    [lsort [array names auxset]]} {
                    break;
                }
            }
            assert {$iterations != 0}

            # PATH 2: positive count (unique behavior) with requested size
            # equal or greater than set size.
            foreach size {50 100} {
                set res [ssdbr srandmember myset $size]
                assert_equal [llength $res] 50
                assert_equal [lsort $res] [lsort [array names myset]]
            }

            # PATH 3: Ask almost as elements as there are in the set.
            # In this case the implementation will duplicate the original
            # set and will remove random elements up to the requested size.
            #
            # PATH 4: Ask a number of elements definitely smaller than
            # the set size.
            #
            # We can test both the code paths just changing the size but
            # using the same code.

            foreach size {45 5} {
                set res [ssdbr srandmember myset $size]
                assert_equal [llength $res] $size

                # 1) Check that all the elements actually belong to the
                # original set.
                foreach ele $res {
                    assert {[info exists myset($ele)]}
                }

                # 2) Check that eventually all the elements are returned.
                unset -nocomplain auxset
                set iterations 1000
                while {$iterations != 0} {
                    incr iterations -1
                    set res [ssdbr srandmember myset -10]
                    foreach ele $res {
                        set auxset($ele) 1
                    }
                    if {[lsort [array names myset]] eq
                        [lsort [array names auxset]]} {
                        break;
                    }
                }
                assert {$iterations != 0}
            }
        }
    }

#    proc setup_move {} {
#        ssdbr del myset3 myset4
#        create_set myset1 {1 a b}
#        create_set myset2 {2 3 4}
#        assert_encoding hashtable myset1
#        assert_encoding intset myset2
#    }
#
#    test "SMOVE basics - from regular set to intset" {
#        # move a non-integer element to an intset should convert encoding
#        setup_move
#        assert_equal 1 [ssdbr smove myset1 myset2 a]
#        assert_equal {1 b} [lsort [ssdbr smembers myset1]]
#        assert_equal {2 3 4 a} [lsort [ssdbr smembers myset2]]
#        assert_encoding hashtable myset2
#
#        # move an integer element should not convert the encoding
#        setup_move
#        assert_equal 1 [ssdbr smove myset1 myset2 1]
#        assert_equal {a b} [lsort [ssdbr smembers myset1]]
#        assert_equal {1 2 3 4} [lsort [ssdbr smembers myset2]]
#        assert_encoding intset myset2
#    }
#
#    test "SMOVE basics - from intset to regular set" {
#        setup_move
#        assert_equal 1 [ssdbr smove myset2 myset1 2]
#        assert_equal {1 2 a b} [lsort [ssdbr smembers myset1]]
#        assert_equal {3 4} [lsort [ssdbr smembers myset2]]
#    }
#
#    test "SMOVE non existing key" {
#        setup_move
#        assert_equal 0 [ssdbr smove myset1 myset2 foo]
#        assert_equal 0 [ssdbr smove myset1 myset1 foo]
#        assert_equal {1 a b} [lsort [ssdbr smembers myset1]]
#        assert_equal {2 3 4} [lsort [ssdbr smembers myset2]]
#    }
#
#    test "SMOVE non existing src set" {
#        setup_move
#        assert_equal 0 [ssdbr smove noset myset2 foo]
#        assert_equal {2 3 4} [lsort [ssdbr smembers myset2]]
#    }
#
#    test "SMOVE from regular set to non existing destination set" {
#        setup_move
#        assert_equal 1 [ssdbr smove myset1 myset3 a]
#        assert_equal {1 b} [lsort [ssdbr smembers myset1]]
#        assert_equal {a} [lsort [ssdbr smembers myset3]]
#        assert_encoding hashtable myset3
#    }
#
#    test "SMOVE from intset to non existing destination set" {
#        setup_move
#        assert_equal 1 [ssdbr smove myset2 myset3 2]
#        assert_equal {3 4} [lsort [ssdbr smembers myset2]]
#        assert_equal {2} [lsort [ssdbr smembers myset3]]
#        assert_encoding intset myset3
#    }
#
#    test "SMOVE wrong src key type" {
#        ssdbr set x 10
#        assert_error "WRONGTYPE*" {r smove x myset2 foo}
#    }
#
#    test "SMOVE wrong dst key type" {
#        ssdbr set x 10
#        assert_error "WRONGTYPE*" {r smove myset2 x foo}
#    }
#
#    test "SMOVE with identical source and destination" {
#        ssdbr del set
#        ssdbr sadd set a b c
#        ssdbr smove set set b
#        lsort [ssdbr smembers set]
#    } {a b c}
#
    tags {slow} {
        test {intsets implementation stress testing} {
            if {$::accurate} {
                set loops 20
            } else {
                set loops 2
            }
            for {set j 0} {$j < 2} {incr j} {
                unset -nocomplain s
                array set s {}
                ssdbr del s
                set len [randomInt 1024]
                for {set i 0} {$i < $len} {incr i} {
                    randpath {
                        set data [randomInt 65536]
                    } {
                        set data [randomInt 4294967296]
                    } {
                        set data [randomInt 18446744073709551616]
                    }
                    set s($data) {}
                    ssdbr sadd s $data
                }
                assert_equal [lsort [ssdbr smembers s]] [lsort [array names s]]
                set len [array size s]
                for {set i 0} {$i < $len} {incr i} {
                    set e [ssdbr spop s]
                    if {![info exists s($e)]} {
                        puts "Can't find '$e' on local array"
                        puts "Local array: [lsort [ssdbr smembers s]]"
                        puts "Remote array: [lsort [array names s]]"
                        error "exception"
                    }
                    array unset s $e
                }
                assert_equal [ssdbr scard s] 0
                assert_equal [array size s] 0
            }
        }
    }
}
