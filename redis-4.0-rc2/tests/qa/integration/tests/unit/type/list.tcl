start_server {
    tags {"list"}
    overrides {
        "list-max-ziplist-size" 5
    }
} {
    source "tests/unit/type/list-common.tcl"

    test {LPUSH, RPUSH, LLENGTH, LINDEX, LPOP - ziplist} {
        # first lpush then rpush
        assert_equal 1 [ssdbr lpush myziplist1 aa]
        assert_equal 2 [ssdbr rpush myziplist1 bb]
        assert_equal 3 [ssdbr rpush myziplist1 cc]
        assert_equal 3 [ssdbr llen myziplist1]
        assert_equal aa [ssdbr lindex myziplist1 0]
        assert_equal bb [ssdbr lindex myziplist1 1]
        assert_equal cc [ssdbr lindex myziplist1 2]
        assert_equal {} [ssdbr lindex myziplist2 3]
        assert_equal cc [ssdbr rpop myziplist1]
        assert_equal aa [ssdbr lpop myziplist1]
        assert_encoding quicklist myziplist1

        # first rpush then lpush
        assert_equal 1 [ssdbr rpush myziplist2 a]
        assert_equal 2 [ssdbr lpush myziplist2 b]
        assert_equal 3 [ssdbr lpush myziplist2 c]
        assert_equal 3 [ssdbr llen myziplist2]
        assert_equal c [ssdbr lindex myziplist2 0]
        assert_equal b [ssdbr lindex myziplist2 1]
        assert_equal a [ssdbr lindex myziplist2 2]
        assert_equal {} [ssdbr lindex myziplist2 3]
        assert_equal a [ssdbr rpop myziplist2]
        assert_equal c [ssdbr lpop myziplist2]
        assert_encoding quicklist myziplist2
    }

    test {LPUSH, RPUSH, LLENGTH, LINDEX, LPOP - regular list} {
        # first lpush then rpush
        assert_equal 1 [ssdbr lpush mylist1 $largevalue(linkedlist)]
        assert_encoding quicklist mylist1
        assert_equal 2 [ssdbr rpush mylist1 b]
        assert_equal 3 [ssdbr rpush mylist1 c]
        assert_equal 3 [ssdbr llen mylist1]
        assert_equal $largevalue(linkedlist) [ssdbr lindex mylist1 0]
        assert_equal b [ssdbr lindex mylist1 1]
        assert_equal c [ssdbr lindex mylist1 2]
        assert_equal {} [ssdbr lindex mylist1 3]
        assert_equal c [ssdbr rpop mylist1]
        assert_equal $largevalue(linkedlist) [ssdbr lpop mylist1]

        # first rpush then lpush
        assert_equal 1 [ssdbr rpush mylist2 $largevalue(linkedlist)]
        assert_encoding quicklist mylist2
        assert_equal 2 [ssdbr lpush mylist2 b]
        assert_equal 3 [ssdbr lpush mylist2 c]
        assert_equal 3 [ssdbr llen mylist2]
        assert_equal c [ssdbr lindex mylist2 0]
        assert_equal b [ssdbr lindex mylist2 1]
        assert_equal $largevalue(linkedlist) [ssdbr lindex mylist2 2]
        assert_equal {} [ssdbr lindex mylist2 3]
        assert_equal $largevalue(linkedlist) [ssdbr rpop mylist2]
        assert_equal c [ssdbr lpop mylist2]
    }

    test {R/LPOP against empty list} {
        ssdbr lpop non-existing-list
    } {}

    test {Variadic RPUSH/LPUSH} {
        ssdbr del mylist
        assert_equal 4 [ssdbr lpush mylist a b c d]
        assert_equal 8 [ssdbr rpush mylist 0 1 2 3]
        assert_equal {d c b a 0 1 2 3} [ssdbr lrange mylist 0 -1]
    }

    test {DEL a list} {
        assert_equal 1 [ssdbr del mylist2]
        assert_equal 0 [ssdbr exists mylist2]
        assert_equal 0 [ssdbr llen mylist2]
    }

    proc create_list {key entries} {
        ssdbr del $key
        foreach entry $entries { r rpush $key $entry }
        assert_encoding quicklist $key
    }

    foreach {type large} [array get largevalue] {
        test "BLPOP, BRPOP: single existing list - $type" {
            set rd [redis_deferring_client]
            create_list blist "a b $large c d"

            $rd blpop blist 1
            assert_equal {blist a} [$rd read]
            $rd brpop blist 1
            assert_equal {blist d} [$rd read]

            $rd blpop blist 1
            assert_equal {blist b} [$rd read]
            $rd brpop blist 1
            assert_equal {blist c} [$rd read]
        }

        test "BLPOP, BRPOP: multiple existing lists - $type" {
            set rd [redis_deferring_client]
            create_list blist1 "a $large c"
            create_list blist2 "d $large f"

            $rd blpop blist1 blist2 1
            assert_equal {blist1 a} [$rd read]
            $rd brpop blist1 blist2 1
            assert_equal {blist1 c} [$rd read]
            assert_equal 1 [ssdbr llen blist1]
            assert_equal 3 [ssdbr llen blist2]

            $rd blpop blist2 blist1 1
            assert_equal {blist2 d} [$rd read]
            $rd brpop blist2 blist1 1
            assert_equal {blist2 f} [$rd read]
            assert_equal 1 [ssdbr llen blist1]
            assert_equal 1 [ssdbr llen blist2]
        }

        test "BLPOP, BRPOP: second list has an entry - $type" {
            set rd [redis_deferring_client]
            ssdbr del blist1
            create_list blist2 "d $large f"

            $rd blpop blist1 blist2 1
            assert_equal {blist2 d} [$rd read]
            $rd brpop blist1 blist2 1
            assert_equal {blist2 f} [$rd read]
            assert_equal 0 [ssdbr llen blist1]
            assert_equal 1 [ssdbr llen blist2]
        }

        test "BRPOPLPUSH - $type" {
            ssdbr del target

            set rd [redis_deferring_client]
            create_list blist "a b $large c d"

            $rd brpoplpush blist target 1
            assert_equal d [$rd read]

            assert_equal d [ssdbr rpop target]
            assert_equal "a b $large c" [ssdbr lrange blist 0 -1]
        }
    }

    test "BLPOP, LPUSH + DEL should not awake blocked client" {
        set rd [redis_deferring_client]
        ssdbr del list

        $rd blpop list 0
        ssdbr multi
        ssdbr lpush list a
        ssdbr del list
        ssdbr exec
        ssdbr del list
        ssdbr lpush list b
        $rd read
    } {list b}

    test "BLPOP, LPUSH + DEL + SET should not awake blocked client" {
        set rd [redis_deferring_client]
        ssdbr del list

        $rd blpop list 0
        ssdbr multi
        ssdbr lpush list a
        ssdbr del list
        ssdbr set list foo
        ssdbr exec
        ssdbr del list
        ssdbr lpush list b
        $rd read
    } {list b}

    test "BLPOP with same key multiple times should work (issue #801)" {
        set rd [redis_deferring_client]
        ssdbr del list1 list2

        # Data arriving after the BLPOP.
        $rd blpop list1 list2 list2 list1 0
        ssdbr lpush list1 a
        assert_equal [$rd read] {list1 a}
        $rd blpop list1 list2 list2 list1 0
        ssdbr lpush list2 b
        assert_equal [$rd read] {list2 b}

        # Data already there.
        ssdbr lpush list1 a
        ssdbr lpush list2 b
        $rd blpop list1 list2 list2 list1 0
        assert_equal [$rd read] {list1 a}
        $rd blpop list1 list2 list2 list1 0
        assert_equal [$rd read] {list2 b}
    }

    test "MULTI/EXEC is isolated from the point of view of BLPOP" {
        set rd [redis_deferring_client]
        ssdbr del list
        $rd blpop list 0
        ssdbr multi
        ssdbr lpush list a
        ssdbr lpush list b
        ssdbr lpush list c
        ssdbr exec
        $rd read
    } {list c}

    test "BLPOP with variadic LPUSH" {
        set rd [redis_deferring_client]
        ssdbr del blist target
        if {$::valgrind} {after 100}
        $rd blpop blist 0
        if {$::valgrind} {after 100}
        assert_equal 2 [ssdbr lpush blist foo bar]
        if {$::valgrind} {after 100}
        assert_equal {blist bar} [$rd read]
        assert_equal foo [lindex [ssdbr lrange blist 0 -1] 0]
    }

    test "BRPOPLPUSH with zero timeout should block indefinitely" {
        set rd [redis_deferring_client]
        ssdbr del blist target
        $rd brpoplpush blist target 0
        after 1000
        ssdbr rpush blist foo
        assert_equal foo [$rd read]
        assert_equal {foo} [ssdbr lrange target 0 -1]
    }

    test "BRPOPLPUSH with a client BLPOPing the target list" {
        set rd [redis_deferring_client]
        set rd2 [redis_deferring_client]
        ssdbr del blist target
        $rd2 blpop target 0
        $rd brpoplpush blist target 0
        after 1000
        ssdbr rpush blist foo
        assert_equal foo [$rd read]
        assert_equal {target foo} [$rd2 read]
        assert_equal 0 [ssdbr exists target]
    }

    test "BRPOPLPUSH with wrong source type" {
        set rd [redis_deferring_client]
        ssdbr del blist target
        ssdbr set blist nolist
        $rd brpoplpush blist target 1
        assert_error "WRONGTYPE*" {$rd read}
    }

    test "BRPOPLPUSH with wrong destination type" {
        set rd [redis_deferring_client]
        ssdbr del blist target
        ssdbr set target nolist
        ssdbr lpush blist foo
        $rd brpoplpush blist target 1
        assert_error "WRONGTYPE*" {$rd read}

        set rd [redis_deferring_client]
        ssdbr del blist target
        ssdbr set target nolist
        $rd brpoplpush blist target 0
        after 1000
        ssdbr rpush blist foo
        assert_error "WRONGTYPE*" {$rd read}
        assert_equal {foo} [ssdbr lrange blist 0 -1]
    }

    test "BRPOPLPUSH maintains order of elements after failure" {
        set rd [redis_deferring_client]
        ssdbr del blist target
        ssdbr set target nolist
        $rd brpoplpush blist target 0
        ssdbr rpush blist a b c
        assert_error "WRONGTYPE*" {$rd read}
        ssdbr lrange blist 0 -1
    } {a b c}

    test "BRPOPLPUSH with multiple blocked clients" {
        set rd1 [redis_deferring_client]
        set rd2 [redis_deferring_client]
        ssdbr del blist target1 target2
        ssdbr set target1 nolist
        $rd1 brpoplpush blist target1 0
        $rd2 brpoplpush blist target2 0
        ssdbr lpush blist foo

        assert_error "WRONGTYPE*" {$rd1 read}
        assert_equal {foo} [$rd2 read]
        assert_equal {foo} [ssdbr lrange target2 0 -1]
    }

    test "Linked BRPOPLPUSH" {
      set rd1 [redis_deferring_client]
      set rd2 [redis_deferring_client]

      ssdbr del list1 list2 list3

      $rd1 brpoplpush list1 list2 0
      $rd2 brpoplpush list2 list3 0

      ssdbr rpush list1 foo

      assert_equal {} [ssdbr lrange list1 0 -1]
      assert_equal {} [ssdbr lrange list2 0 -1]
      assert_equal {foo} [ssdbr lrange list3 0 -1]
    }

    test "Circular BRPOPLPUSH" {
      set rd1 [redis_deferring_client]
      set rd2 [redis_deferring_client]

      ssdbr del list1 list2

      $rd1 brpoplpush list1 list2 0
      $rd2 brpoplpush list2 list1 0

      ssdbr rpush list1 foo

      assert_equal {foo} [ssdbr lrange list1 0 -1]
      assert_equal {} [ssdbr lrange list2 0 -1]
    }

    test "Self-referential BRPOPLPUSH" {
      set rd [redis_deferring_client]

      ssdbr del blist

      $rd brpoplpush blist blist 0

      ssdbr rpush blist foo

      assert_equal {foo} [ssdbr lrange blist 0 -1]
    }

    test "BRPOPLPUSH inside a transaction" {
        ssdbr del xlist target
        ssdbr lpush xlist foo
        ssdbr lpush xlist bar

        ssdbr multi
        ssdbr brpoplpush xlist target 0
        ssdbr brpoplpush xlist target 0
        ssdbr brpoplpush xlist target 0
        ssdbr lrange xlist 0 -1
        ssdbr lrange target 0 -1
        ssdbr exec
    } {foo bar {} {} {bar foo}}

    test "PUSH resulting from BRPOPLPUSH affect WATCH" {
        set blocked_client [redis_deferring_client]
        set watching_client [redis_deferring_client]
        ssdbr del srclist dstlist somekey
        ssdbr set somekey somevalue
        $blocked_client brpoplpush srclist dstlist 0
        $watching_client watch dstlist
        $watching_client read
        $watching_client multi
        $watching_client read
        $watching_client get somekey
        $watching_client read
        ssdbr lpush srclist element
        $watching_client exec
        $watching_client read
    } {}

    test "BRPOPLPUSH does not affect WATCH while still blocked" {
        set blocked_client [redis_deferring_client]
        set watching_client [redis_deferring_client]
        ssdbr del srclist dstlist somekey
        ssdbr set somekey somevalue
        $blocked_client brpoplpush srclist dstlist 0
        $watching_client watch dstlist
        $watching_client read
        $watching_client multi
        $watching_client read
        $watching_client get somekey
        $watching_client read
        $watching_client exec
        # Blocked BLPOPLPUSH may create problems, unblock it.
        ssdbr lpush srclist element
        $watching_client read
    } {somevalue}

    test {BRPOPLPUSH timeout} {
      set rd [redis_deferring_client]

      $rd brpoplpush foo_list bar_list 1
      after 2000
      $rd read
    } {}

    test "BLPOP when new key is moved into place" {
        set rd [redis_deferring_client]

        $rd blpop foo 5
        ssdbr lpush bob abc def hij
        ssdbr rename bob foo
        $rd read
    } {foo hij}

    test "BLPOP when result key is created by SORT..STORE" {
        set rd [redis_deferring_client]

        # zero out list from previous test without explicit delete
        ssdbr lpop foo
        ssdbr lpop foo
        ssdbr lpop foo

        $rd blpop foo 5
        ssdbr lpush notfoo hello hola aguacate konichiwa zanzibar
        ssdbr sort notfoo ALPHA store foo
        $rd read
    } {foo aguacate}

    foreach {pop} {BLPOP BRPOP} {
        test "$pop: with single empty list argument" {
            set rd [redis_deferring_client]
            ssdbr del blist1
            $rd $pop blist1 1
            ssdbr rpush blist1 foo
            assert_equal {blist1 foo} [$rd read]
            assert_equal 0 [ssdbr exists blist1]
        }

        test "$pop: with negative timeout" {
            set rd [redis_deferring_client]
            $rd $pop blist1 -1
            assert_error "ERR*is negative*" {$rd read}
        }

        test "$pop: with non-integer timeout" {
            set rd [redis_deferring_client]
            $rd $pop blist1 1.1
            assert_error "ERR*not an integer*" {$rd read}
        }

        test "$pop: with zero timeout should block indefinitely" {
            # To test this, use a timeout of 0 and wait a second.
            # The blocking pop should still be waiting for a push.
            set rd [redis_deferring_client]
            $rd $pop blist1 0
            after 1000
            ssdbr rpush blist1 foo
            assert_equal {blist1 foo} [$rd read]
        }

        test "$pop: second argument is not a list" {
            set rd [redis_deferring_client]
            ssdbr del blist1 blist2
            ssdbr set blist2 nolist
            $rd $pop blist1 blist2 1
            assert_error "WRONGTYPE*" {$rd read}
        }

        test "$pop: timeout" {
            set rd [redis_deferring_client]
            ssdbr del blist1 blist2
            $rd $pop blist1 blist2 1
            assert_equal {} [$rd read]
        }

        test "$pop: arguments are empty" {
            set rd [redis_deferring_client]
            ssdbr del blist1 blist2

            $rd $pop blist1 blist2 1
            ssdbr rpush blist1 foo
            assert_equal {blist1 foo} [$rd read]
            assert_equal 0 [ssdbr exists blist1]
            assert_equal 0 [ssdbr exists blist2]

            $rd $pop blist1 blist2 1
            ssdbr rpush blist2 foo
            assert_equal {blist2 foo} [$rd read]
            assert_equal 0 [ssdbr exists blist1]
            assert_equal 0 [ssdbr exists blist2]
        }
    }

    test {BLPOP inside a transaction} {
        ssdbr del xlist
        ssdbr lpush xlist foo
        ssdbr lpush xlist bar
        ssdbr multi
        ssdbr blpop xlist 0
        ssdbr blpop xlist 0
        ssdbr blpop xlist 0
        ssdbr exec
    } {{xlist bar} {xlist foo} {}}

    test {LPUSHX, RPUSHX - generic} {
        ssdbr del xlist
        assert_equal 0 [ssdbr lpushx xlist a]
        assert_equal 0 [ssdbr llen xlist]
        assert_equal 0 [ssdbr rpushx xlist a]
        assert_equal 0 [ssdbr llen xlist]
    }

    foreach {type large} [array get largevalue] {
        test "LPUSHX, RPUSHX - $type" {
            create_list xlist "$large c"
            assert_equal 3 [ssdbr rpushx xlist d]
            assert_equal 4 [ssdbr lpushx xlist a]
            assert_equal 6 [ssdbr rpushx xlist 42 x]
            assert_equal 9 [ssdbr lpushx xlist y3 y2 y1]
            assert_equal "y1 y2 y3 a $large c d 42 x" [ssdbr lrange xlist 0 -1]
        }

        test "LINSERT - $type" {
            create_list xlist "a $large c d"
            assert_equal 5 [ssdbr linsert xlist before c zz] "before c"
            assert_equal "a $large zz c d" [ssdbr lrange xlist 0 10] "lrangeA"
            assert_equal 6 [ssdbr linsert xlist after c yy] "after c"
            assert_equal "a $large zz c yy d" [ssdbr lrange xlist 0 10] "lrangeB"
            assert_equal 7 [ssdbr linsert xlist after d dd] "after d"
            assert_equal -1 [ssdbr linsert xlist after bad ddd] "after bad"
            assert_equal "a $large zz c yy d dd" [ssdbr lrange xlist 0 10] "lrangeC"
            assert_equal 8 [ssdbr linsert xlist before a aa] "before a"
            assert_equal -1 [ssdbr linsert xlist before bad aaa] "before bad"
            assert_equal "aa a $large zz c yy d dd" [ssdbr lrange xlist 0 10] "lrangeD"

            # check inserting integer encoded value
            assert_equal 9 [ssdbr linsert xlist before aa 42] "before aa"
            assert_equal 42 [ssdbr lrange xlist 0 0] "lrangeE"
        }
    }

    test {LINSERT raise error on bad syntax} {
        catch {[ssdbr linsert xlist aft3r aa 42]} e
        set e
    } {*ERR*syntax*error*}

    foreach {type num} {quicklist 250 quicklist 500} {
        proc check_numbered_list_consistency {key} {
            set len [ssdbr llen $key]
            for {set i 0} {$i < $len} {incr i} {
                assert_equal $i [ssdbr lindex $key $i]
                assert_equal [expr $len-1-$i] [ssdbr lindex $key [expr (-$i)-1]]
            }
        }

        proc check_random_access_consistency {key} {
            set len [ssdbr llen $key]
            for {set i 0} {$i < $len} {incr i} {
                set rint [expr int(rand()*$len)]
                assert_equal $rint [ssdbr lindex $key $rint]
                assert_equal [expr $len-1-$rint] [ssdbr lindex $key [expr (-$rint)-1]]
            }
        }

        test "LINDEX consistency test - $type" {
            ssdbr del mylist
            for {set i 0} {$i < $num} {incr i} {
                ssdbr rpush mylist $i
            }
            assert_encoding $type mylist
            check_numbered_list_consistency mylist
        }

        test "LINDEX random access - $type" {
            assert_encoding $type mylist
            check_random_access_consistency mylist
        }

        test "Check if list is still ok after a DEBUG RELOAD - $type" {
            ssdbr debug reload
            assert_encoding $type mylist
            check_numbered_list_consistency mylist
            check_random_access_consistency mylist
        }
    }

    test {LLEN against non-list value error} {
        ssdbr del mylist
        ssdbr set mylist foobar
        assert_error WRONGTYPE* {r llen mylist}
    }

    test {LLEN against non existing key} {
        assert_equal 0 [ssdbr llen not-a-key]
    }

    test {LINDEX against non-list value error} {
        assert_error WRONGTYPE* {r lindex mylist 0}
    }

    test {LINDEX against non existing key} {
        assert_equal "" [ssdbr lindex not-a-key 10]
    }

    test {LPUSH against non-list value error} {
        assert_error WRONGTYPE* {r lpush mylist 0}
    }

    test {RPUSH against non-list value error} {
        assert_error WRONGTYPE* {r rpush mylist 0}
    }

    foreach {type large} [array get largevalue] {
        test "RPOPLPUSH base case - $type" {
            ssdbr del mylist1 mylist2
            create_list mylist1 "a $large c d"
            assert_equal d [ssdbr rpoplpush mylist1 mylist2]
            assert_equal c [ssdbr rpoplpush mylist1 mylist2]
            assert_equal "a $large" [ssdbr lrange mylist1 0 -1]
            assert_equal "c d" [ssdbr lrange mylist2 0 -1]
            assert_encoding quicklist mylist2
        }

        test "RPOPLPUSH with the same list as src and dst - $type" {
            create_list mylist "a $large c"
            assert_equal "a $large c" [ssdbr lrange mylist 0 -1]
            assert_equal c [ssdbr rpoplpush mylist mylist]
            assert_equal "c a $large" [ssdbr lrange mylist 0 -1]
        }

        foreach {othertype otherlarge} [array get largevalue] {
            test "RPOPLPUSH with $type source and existing target $othertype" {
                create_list srclist "a b c $large"
                create_list dstlist "$otherlarge"
                assert_equal $large [ssdbr rpoplpush srclist dstlist]
                assert_equal c [ssdbr rpoplpush srclist dstlist]
                assert_equal "a b" [ssdbr lrange srclist 0 -1]
                assert_equal "c $large $otherlarge" [ssdbr lrange dstlist 0 -1]

                # When we rpoplpush'ed a large value, dstlist should be
                # converted to the same encoding as srclist.
                if {$type eq "linkedlist"} {
                    assert_encoding quicklist dstlist
                }
            }
        }
    }

    test {RPOPLPUSH against non existing key} {
        ssdbr del srclist dstlist
        assert_equal {} [ssdbr rpoplpush srclist dstlist]
        assert_equal 0 [ssdbr exists srclist]
        assert_equal 0 [ssdbr exists dstlist]
    }

    test {RPOPLPUSH against non list src key} {
        ssdbr del srclist dstlist
        ssdbr set srclist x
        assert_error WRONGTYPE* {r rpoplpush srclist dstlist}
        assert_type string srclist
        assert_equal 0 [ssdbr exists newlist]
    }

    test {RPOPLPUSH against non list dst key} {
        create_list srclist {a b c d}
        ssdbr set dstlist x
        assert_error WRONGTYPE* {r rpoplpush srclist dstlist}
        assert_type string dstlist
        assert_equal {a b c d} [ssdbr lrange srclist 0 -1]
    }

    test {RPOPLPUSH against non existing src key} {
        ssdbr del srclist dstlist
        assert_equal {} [ssdbr rpoplpush srclist dstlist]
    } {}

    foreach {type large} [array get largevalue] {
        test "Basic LPOP/RPOP - $type" {
            create_list mylist "$large 1 2"
            assert_equal $large [ssdbr lpop mylist]
            assert_equal 2 [ssdbr rpop mylist]
            assert_equal 1 [ssdbr lpop mylist]
            assert_equal 0 [ssdbr llen mylist]

            # pop on empty list
            assert_equal {} [ssdbr lpop mylist]
            assert_equal {} [ssdbr rpop mylist]
        }
    }

    test {LPOP/RPOP against non list value} {
        ssdbr set notalist foo
        assert_error WRONGTYPE* {r lpop notalist}
        assert_error WRONGTYPE* {r rpop notalist}
    }

    foreach {type num} {quicklist 250 quicklist 500} {
        test "Mass RPOP/LPOP - $type" {
            ssdbr del mylist
            set sum1 0
            for {set i 0} {$i < $num} {incr i} {
                ssdbr lpush mylist $i
                incr sum1 $i
            }
            assert_encoding $type mylist
            set sum2 0
            for {set i 0} {$i < [expr $num/2]} {incr i} {
                incr sum2 [ssdbr lpop mylist]
                incr sum2 [ssdbr rpop mylist]
            }
            assert_equal $sum1 $sum2
        }
    }

    foreach {type large} [array get largevalue] {
        test "LRANGE basics - $type" {
            create_list mylist "$large 1 2 3 4 5 6 7 8 9"
            assert_equal {1 2 3 4 5 6 7 8} [ssdbr lrange mylist 1 -2]
            assert_equal {7 8 9} [ssdbr lrange mylist -3 -1]
            assert_equal {4} [ssdbr lrange mylist 4 4]
        }

        test "LRANGE inverted indexes - $type" {
            create_list mylist "$large 1 2 3 4 5 6 7 8 9"
            assert_equal {} [ssdbr lrange mylist 6 2]
        }

        test "LRANGE out of range indexes including the full list - $type" {
            create_list mylist "$large 1 2 3"
            assert_equal "$large 1 2 3" [ssdbr lrange mylist -1000 1000]
        }

        test "LRANGE out of range negative end index - $type" {
            create_list mylist "$large 1 2 3"
            assert_equal $large [ssdbr lrange mylist 0 -4]
            assert_equal {} [ssdbr lrange mylist 0 -5]
        }
    }

    test {LRANGE against non existing key} {
        assert_equal {} [ssdbr lrange nosuchkey 0 1]
    }

    foreach {type large} [array get largevalue] {
        proc trim_list {type min max} {
            upvar 1 large large
            ssdbr del mylist
            create_list mylist "1 2 3 4 $large"
            ssdbr ltrim mylist $min $max
            ssdbr lrange mylist 0 -1
        }

        test "LTRIM basics - $type" {
            assert_equal "1" [trim_list $type 0 0]
            assert_equal "1 2" [trim_list $type 0 1]
            assert_equal "1 2 3" [trim_list $type 0 2]
            assert_equal "2 3" [trim_list $type 1 2]
            assert_equal "2 3 4 $large" [trim_list $type 1 -1]
            assert_equal "2 3 4" [trim_list $type 1 -2]
            assert_equal "4 $large" [trim_list $type -2 -1]
            assert_equal "$large" [trim_list $type -1 -1]
            assert_equal "1 2 3 4 $large" [trim_list $type -5 -1]
            assert_equal "1 2 3 4 $large" [trim_list $type -10 10]
            assert_equal "1 2 3 4 $large" [trim_list $type 0 5]
            assert_equal "1 2 3 4 $large" [trim_list $type 0 10]
        }

        test "LTRIM out of range negative end index - $type" {
            assert_equal {1} [trim_list $type 0 -5]
            assert_equal {} [trim_list $type 0 -6]
        }

    }

    foreach {type large} [array get largevalue] {
        test "LSET - $type" {
            create_list mylist "99 98 $large 96 95"
            ssdbr lset mylist 1 foo
            ssdbr lset mylist -1 bar
            assert_equal "99 foo $large 96 bar" [ssdbr lrange mylist 0 -1]
        }

        test "LSET out of range index - $type" {
            assert_error ERR*range* {r lset mylist 10 foo}
        }
    }

    test {LSET against non existing key} {
        assert_error ERR*key* {r lset nosuchkey 10 foo}
    }

    test {LSET against non list value} {
        ssdbr set nolist foobar
        assert_error WRONGTYPE* {r lset nolist 0 foo}
    }

    foreach {type e} [array get largevalue] {
        test "LREM remove all the occurrences - $type" {
            create_list mylist "$e foo bar foobar foobared zap bar test foo"
            assert_equal 2 [ssdbr lrem mylist 0 bar]
            assert_equal "$e foo foobar foobared zap test foo" [ssdbr lrange mylist 0 -1]
        }

        test "LREM remove the first occurrence - $type" {
            assert_equal 1 [ssdbr lrem mylist 1 foo]
            assert_equal "$e foobar foobared zap test foo" [ssdbr lrange mylist 0 -1]
        }

        test "LREM remove non existing element - $type" {
            assert_equal 0 [ssdbr lrem mylist 1 nosuchelement]
            assert_equal "$e foobar foobared zap test foo" [ssdbr lrange mylist 0 -1]
        }

        test "LREM starting from tail with negative count - $type" {
            create_list mylist "$e foo bar foobar foobared zap bar test foo foo"
            assert_equal 1 [ssdbr lrem mylist -1 bar]
            assert_equal "$e foo bar foobar foobared zap test foo foo" [ssdbr lrange mylist 0 -1]
        }

        test "LREM starting from tail with negative count (2) - $type" {
            assert_equal 2 [ssdbr lrem mylist -2 foo]
            assert_equal "$e foo bar foobar foobared zap test" [ssdbr lrange mylist 0 -1]
        }

        test "LREM deleting objects that may be int encoded - $type" {
            create_list myotherlist "$e 1 2 3"
            assert_equal 1 [ssdbr lrem myotherlist 1 2]
            assert_equal 3 [ssdbr llen myotherlist]
        }
    }

    test "Regression for bug 593 - chaining BRPOPLPUSH with other blocking cmds" {
        set rd1 [redis_deferring_client]
        set rd2 [redis_deferring_client]

        $rd1 brpoplpush a b 0
        $rd1 brpoplpush a b 0
        $rd2 brpoplpush b c 0
        after 1000
        ssdbr lpush a data
        $rd1 close
        $rd2 close
        ssdbr ping
    } {PONG}
}
