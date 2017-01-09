source "./ssdb/init_ssdb.tcl"
start_server {
    tags {"ssdb"}
    overrides {
        "list-max-ziplist-size" 5
    }
} {
    source "ssdb/list-common.tcl"

    test {LPUSH, RPUSH, LLENGTH, LINDEX, LPOP - ziplist} {
        # first lpush then rpush
        r del myziplist1
        assert_equal 1 [r lpush myziplist1 aa]
        assert_equal 2 [r rpush myziplist1 bb]
        assert_equal 3 [r rpush myziplist1 cc]
        assert_equal 3 [r llen myziplist1]
        assert_equal aa [r lindex myziplist1 0]
        assert_equal bb [r lindex myziplist1 1]
        assert_equal cc [r lindex myziplist1 2]
        assert_equal {} [r lindex myziplist2 3]
        assert_equal cc [r rpop myziplist1]
        assert_equal aa [r lpop myziplist1]
        assert_encoding quicklist myziplist1

        # first rpush then lpush
        r del myziplist2
        assert_equal 1 [r rpush myziplist2 a]
        assert_equal 2 [r lpush myziplist2 b]
        assert_equal 3 [r lpush myziplist2 c]
        assert_equal 3 [r llen myziplist2]
        assert_equal c [r lindex myziplist2 0]
        assert_equal b [r lindex myziplist2 1]
        assert_equal a [r lindex myziplist2 2]
        assert_equal {} [r lindex myziplist2 3]
        assert_equal a [r rpop myziplist2]
        assert_equal c [r lpop myziplist2]
        assert_encoding quicklist myziplist2
    }

    test {LPUSH, RPUSH, LLENGTH, LINDEX, LPOP - regular list} {
        # first lpush then rpush
        r del mylist1
        assert_equal 1 [r lpush mylist1 $largevalue(linkedlist)]
        assert_encoding quicklist mylist1
        assert_equal 2 [r rpush mylist1 b]
        assert_equal 3 [r rpush mylist1 c]
        assert_equal 3 [r llen mylist1]
        assert_equal $largevalue(linkedlist) [r lindex mylist1 0]
        assert_equal b [r lindex mylist1 1]
        assert_equal c [r lindex mylist1 2]
        assert_equal {} [r lindex mylist1 3]
        assert_equal c [r rpop mylist1]
        assert_equal $largevalue(linkedlist) [r lpop mylist1]

        # first rpush then lpush
        r del mylist2
        assert_equal 1 [r rpush mylist2 $largevalue(linkedlist)]
        assert_encoding quicklist mylist2
        assert_equal 2 [r lpush mylist2 b]
        assert_equal 3 [r lpush mylist2 c]
        assert_equal 3 [r llen mylist2]
        assert_equal c [r lindex mylist2 0]
        assert_equal b [r lindex mylist2 1]
        assert_equal $largevalue(linkedlist) [r lindex mylist2 2]
        assert_equal {} [r lindex mylist2 3]
        assert_equal $largevalue(linkedlist) [r rpop mylist2]
        assert_equal c [r lpop mylist2]
    }

    test {R/LPOP against empty list} {
        r lpop non-existing-list
    } {}

    test {Variadic RPUSH/LPUSH} {
        r del mylist
        assert_equal 4 [r lpush mylist a b c d]
        assert_equal 8 [r rpush mylist 0 1 2 3]
        assert_equal {d c b a 0 1 2 3} [r lrange mylist 0 -1]
    }

    test {DEL a list} {
        assert_equal 1 [r del mylist2]
        assert_equal 0 [r exists mylist2]
        assert_equal 0 [r llen mylist2]
    }

    proc create_list {key entries} {
        r del $key
        foreach entry $entries { r rpush $key $entry }
        assert_encoding quicklist $key
    }



    foreach {type num} {quicklist 250 quicklist 500} {
        proc check_numbered_list_consistency {key} {
            set len [r llen $key]
            for {set i 0} {$i < $len} {incr i} {
                assert_equal $i [r lindex $key $i]
                assert_equal [expr $len-1-$i] [r lindex $key [expr (-$i)-1]]
            }
        }

        proc check_random_access_consistency {key} {
            set len [r llen $key]
            for {set i 0} {$i < $len} {incr i} {
                set rint [expr int(rand()*$len)]
                assert_equal $rint [r lindex $key $rint]
                assert_equal [expr $len-1-$rint] [r lindex $key [expr (-$rint)-1]]
            }
        }

        test "LINDEX consistency test - $type" {
            r del mylist
            for {set i 0} {$i < $num} {incr i} {
                r rpush mylist $i
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
        r del mylist
        r set mylist foobar
        assert_error ERR* {r llen mylist}
    }

    test {LLEN against non existing key} {
        assert_equal 0 [r llen not-a-key]
    }

    test {LINDEX against non-list value error} {
        assert_error ERR* {r lindex mylist 0}
    }

    test {LINDEX against non existing key} {
        assert_equal "" [r lindex not-a-key 10]
    }

    test {LPUSH against non-list value error} {
        assert_error ERR* {r lpush mylist 0}
    }

    test {RPUSH against non-list value error} {
        assert_error ERR* {r rpush mylist 0}
    }

    foreach {type large} [array get largevalue] {
        test "Basic LPOP/RPOP - $type" {
            create_list mylist "$large 1 2"
            assert_equal $large [r lpop mylist]
            assert_equal 2 [r rpop mylist]
            assert_equal 1 [r lpop mylist]
            assert_equal 0 [r llen mylist]

            # pop on empty list
            assert_equal {} [r lpop mylist]
            assert_equal {} [r rpop mylist]
        }
    }

    test {LPOP/RPOP against non list value} {
        r set notalist foo
        assert_error ERR* {r lpop notalist}
        assert_error ERR* {r rpop notalist}
    }

    foreach {type num} {quicklist 250 quicklist 500} {
        test "Mass RPOP/LPOP - $type" {
            r del mylist
            set sum1 0
            for {set i 0} {$i < $num} {incr i} {
                r lpush mylist $i
                incr sum1 $i
            }
            assert_encoding $type mylist
            set sum2 0
            for {set i 0} {$i < [expr $num/2]} {incr i} {
                incr sum2 [r lpop mylist]
                incr sum2 [r rpop mylist]
            }
            assert_equal $sum1 $sum2
        }
    }

    foreach {type large} [array get largevalue] {
        test "LRANGE basics - $type" {
            create_list mylist "$large 1 2 3 4 5 6 7 8 9"
            assert_equal {1 2 3 4 5 6 7 8} [r lrange mylist 1 -2]
            assert_equal {7 8 9} [r lrange mylist -3 -1]
            assert_equal {4} [r lrange mylist 4 4]
        }

        test "LRANGE inverted indexes - $type" {
            create_list mylist "$large 1 2 3 4 5 6 7 8 9"

            assert_equal {} [r lrange mylist 6 2]
        }

        test "LRANGE out of range indexes including the full list - $type" {
            create_list mylist "$large 1 2 3"
            assert_equal "$large 1 2 3" [r lrange mylist -1000 1000]
        }

        test "LRANGE out of range negative end index - $type" {
            create_list mylist "$large 1 2 3"
            assert_equal $large [r lrange mylist 0 -4]

            assert_equal {} [r lrange mylist 0 -5]
        }
    }

    test {LRANGE against non existing key} {
        assert_equal {} [r lrange nosuchkey 0 1]
    }

    foreach {type large} [array get largevalue] {
        test "LSET - $type" {
            create_list mylist "99 98 $large 96 95"
            r lset mylist 1 foo
            r lset mylist -1 bar
            assert_equal "99 foo $large 96 bar" [r lrange mylist 0 -1]
        }

        test "LSET out of range index - $type" {
            assert_error ERR* {r lset mylist 10 foo}
        }
    }

    test {LSET against non existing key} {
        assert_error ERR* {r lset nosuchkey 10 foo}
    }

    test {LSET against non list value} {
        r set nolist foobar
        assert_error ERR* {r lset nolist 0 foo}
    }
    r del mylist
}
