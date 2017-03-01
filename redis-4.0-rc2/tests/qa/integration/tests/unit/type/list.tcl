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
            assert_equal 3 [r rpushx xlist d]
            assert_equal 4 [r lpushx xlist a]
            assert_equal 6 [r rpushx xlist 42 x]
            assert_equal 9 [r lpushx xlist y3 y2 y1]
            assert_equal "y1 y2 y3 a $large c d 42 x" [r lrange xlist 0 -1]
        }
    }

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
        assert_equal {} [r lrange nosuchkey 0 1]
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
    r del mylist
}
