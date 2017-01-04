source "./ssdb/init_ssdb.tcl"
start_server {tags {"ssdb"}} {
    proc create_zset {key items} {
        r del $key
        foreach {score entry} $items {
            r zadd $key $score $entry
        }
    }

    proc basics {encoding} {

        test "ZSET basic ZADD and score update - $encoding" {
            r del ztmp
            r zadd ztmp 10 x
            r zadd ztmp 20 y
            r zadd ztmp 30 z
            assert_equal {x y z} [r zrange ztmp 0 -1]

            r zadd ztmp 1 y
            assert_equal {y x z} [r zrange ztmp 0 -1]
        }

        test "ZSET element can't be set to NaN with ZADD - $encoding" {
            catch {r zadd myzset nan abc} err
            set err
        } {ERR*}

        test "ZSET element can't be set to NaN with ZINCRBY" {
            catch {r zadd myzset nan abc} err
            set err
        } {ERR*}

        test "ZADD with options syntax error with incomplete pair" {
            r del ztmp
            catch {r zadd ztmp xx 10 x 20} err
            set err
        } {ERR*}

        test "ZADD XX option without key - $encoding" {
            r del ztmp
            assert {[r zadd ztmp xx 10 x] == 0}
            assert {[r zcard ztmp] == 0}
        }

        test "ZADD XX existing key - $encoding" {
            r del ztmp
            r zadd ztmp 10 x
            assert {[r zadd ztmp xx 20 y] == 0}
            assert {[r zcard ztmp] == 1}
        }

        test "ZADD XX returns the number of elements actually added" {
            r del ztmp
            r zadd ztmp 10 x
            set retval [r zadd ztmp 10 x 20 y 30 z]
            assert {$retval == 2}
        }

        test "ZADD XX updates existing elements score" {
            r del ztmp
            r zadd ztmp 10 x 20 y 30 z
            r zadd ztmp xx 5 foo 11 x 21 y 40 zap
            assert {[r zcard ztmp] == 3}
            assert {[r zscore ztmp x] == 11}
            assert {[r zscore ztmp y] == 21}
        }

        test "ZADD XX and NX are not compatible" {
            r del ztmp
            catch {r zadd ztmp xx nx 10 x} err
            set err
        } {ERR*}

        test "ZADD NX with non exisitng key" {
            r del ztmp
            r zadd ztmp nx 10 x 20 y 30 z
            assert {[r zcard ztmp] == 3}
        }

        test "ZADD NX only add new elements without updating old ones" {
            r del ztmp
            r zadd ztmp 10 x 20 y 30 z
            assert {[r zadd ztmp nx 11 x 21 y 100 a 200 b] == 2}
            assert {[r zscore ztmp x] == 10}
            assert {[r zscore ztmp y] == 20}
            assert {[r zscore ztmp a] == 100}
            assert {[r zscore ztmp b] == 200}
        }

        test "ZADD INCR works like ZINCRBY" {
            r del ztmp
            r zadd ztmp 10 x 20 y 30 z
            r zadd ztmp INCR 15 x
            assert {[r zscore ztmp x] == 25}
        }

        test "ZADD INCR works with a single score-elemenet pair" {
            r del ztmp
            r zadd ztmp 10 x 20 y 30 z
            catch {r zadd ztmp INCR 15 x 10 y} err
            set err
        } {ERR*}

        test "ZADD CH option changes return value to all changed elements" {
            r del ztmp
            r zadd ztmp 10 x 20 y 30 z
            assert {[r zadd ztmp 11 x 21 y 30 z] == 0}
            assert {[r zadd ztmp ch 12 x 22 y 30 z] == 2}
        }

        test "ZINCRBY calls leading to NaN result in error" {
            r del myzset
            catch {r zincrby myzset +inf abc} err
            assert_equal inf [r zscore myzset abc]
            catch {r zincrby myzset -inf abc} err
            set err
        } {ERR*}

        test {ZADD - Variadic version base case} {
            r del myzset
            list [r zadd myzset 10 a 20 b 30 c] [r zrange myzset 0 -1 withscores]
        } {3 {a 10 b 20 c 30}}

        test {ZADD - Return value is the number of actually added items} {
            list [r zadd myzset 5 x 20 b 30 c] [r zrange myzset 0 -1 withscores]
        } {1 {x 5 a 10 b 20 c 30}}

        test {ZADD - Variadic version does not add nothing on single parsing err} {
            r del myzset
            catch {r zadd myzset 10 a 20 b 30.badscore c} e
            list [set e] [r zcard myzset]
        } {ERR* 0}

        test {ZADD - Variadic version will raise error on missing arg} {
            r del myzset
            catch {r zadd myzset 10 a 20 b 30 c 40} e
            set e
        } {ERR*}

        test {ZINCRBY does not work variadic even if shares ZADD implementation} {
            r del myzset
            catch {r zincrby myzset 10 a 20 b 30 c} e
            assert_match {*ERR*wrong*number*arg*} $e
        }

        test "ZCARD basics - $encoding" {
            r del ztmp
            r zadd ztmp 10 a 20 b 30 c
            assert_equal 3 [r zcard ztmp]
            assert_equal 0 [r zcard zdoesntexist]
        }

        test "ZREM removes key after last element is removed" {
            r del ztmp
            r zadd ztmp 10 x
            r zadd ztmp 20 y

            assert_equal 2 [r zcard ztmp]
            assert_equal 0 [r zrem ztmp z]
            assert_equal 1 [r zrem ztmp y]
            assert_equal 1 [r zrem ztmp x]
            assert_equal 0 [r zcard ztmp]
        }

        test "ZREM variadic version" {
            r del ztmp
            r zadd ztmp 10 a 20 b 30 c
            assert_equal 2 [r zrem ztmp x y a b k]
            assert_equal 0 [r zrem ztmp foo bar]
            assert_equal 1 [r zrem ztmp c]
            r exists ztmp
        } {0}

        test "ZREM variadic version -- remove elements after key deletion" {
            r del ztmp
            r zadd ztmp 10 a 20 b 30 c
            r zrem ztmp a b c d e f g
        } {3}

        #TODO zrange ztmp 0 -2 will crash test currently
        test "ZRANGE basics - $encoding" {
            r del ztmp
            r zadd ztmp 1 a
            r zadd ztmp 2 b
            r zadd ztmp 3 c
            r zadd ztmp 4 d

            assert_equal {a b c d} [r zrange ztmp 0 -1]
            assert_equal {a b c} [r zrange ztmp 0 -2]
            assert_equal {b c d} [r zrange ztmp 1 -1]
            assert_equal {b c} [r zrange ztmp 1 -2]
            assert_equal {c d} [r zrange ztmp -2 -1]
            assert_equal {c} [r zrange ztmp -2 -2]

            # out of range start index
            assert_equal {a b c} [r zrange ztmp -5 2]
            assert_equal {a b} [r zrange ztmp -5 1]
            assert_equal {} [r zrange ztmp 5 -1]
            assert_equal {} [r zrange ztmp 5 -2]

            # out of range end index
            assert_equal {a b c d} [r zrange ztmp 0 5]
            assert_equal {b c d} [r zrange ztmp 1 5]
            assert_equal {} [r zrange ztmp 0 -5]
            assert_equal {} [r zrange ztmp 1 -5]

            # withscores
            assert_equal {a 1 b 2 c 3 d 4} [r zrange ztmp 0 -1 withscores]
        }

        test "ZREVRANGE basics - $encoding" {
            r del ztmp
            r zadd ztmp 1 a
            r zadd ztmp 2 b
            r zadd ztmp 3 c
            r zadd ztmp 4 d

            assert_equal {d c b a} [r zrevrange ztmp 0 -1]
            assert_equal {d c b} [r zrevrange ztmp 0 -2]
            assert_equal {c b a} [r zrevrange ztmp 1 -1]
            assert_equal {c b} [r zrevrange ztmp 1 -2]
            assert_equal {b a} [r zrevrange ztmp -2 -1]
            assert_equal {b} [r zrevrange ztmp -2 -2]

            # out of range start index
            assert_equal {d c b} [r zrevrange ztmp -5 2]
            assert_equal {d c} [r zrevrange ztmp -5 1]
            assert_equal {} [r zrevrange ztmp 5 -1]
            assert_equal {} [r zrevrange ztmp 5 -2]

            # out of range end index
            assert_equal {d c b a} [r zrevrange ztmp 0 5]
            assert_equal {c b a} [r zrevrange ztmp 1 5]
            assert_equal {} [r zrevrange ztmp 0 -5]
            assert_equal {} [r zrevrange ztmp 1 -5]

            # withscores
            assert_equal {d 4 c 3 b 2 a 1} [r zrevrange ztmp 0 -1 withscores]
        }

        test "ZRANK/ZREVRANK basics - $encoding" {
            r del zranktmp
            r zadd zranktmp 10 x
            r zadd zranktmp 20 y
            r zadd zranktmp 30 z
            assert_equal 0 [r zrank zranktmp x]
            assert_equal 1 [r zrank zranktmp y]
            assert_equal 2 [r zrank zranktmp z]
            assert_equal "" [r zrank zranktmp foo]
            assert_equal 2 [r zrevrank zranktmp x]
            assert_equal 1 [r zrevrank zranktmp y]
            assert_equal 0 [r zrevrank zranktmp z]
            assert_equal "" [r zrevrank zranktmp foo]
        }

        test "ZRANK - after deletion - $encoding" {
            r zrem zranktmp y
            assert_equal 0 [r zrank zranktmp x]
            assert_equal 1 [r zrank zranktmp z]
        }

        test "ZINCRBY - can create a new sorted set - $encoding" {
            r del zset
            r zincrby zset 1 foo
            assert_equal {foo} [r zrange zset 0 -1]
            assert_equal 1 [r zscore zset foo]
        }

        test "ZINCRBY - increment and decrement - $encoding" {
            r zincrby zset 2 foo
            r zincrby zset 1 bar
            assert_equal {bar foo} [r zrange zset 0 -1]

            r zincrby zset 10 bar
            r zincrby zset -5 foo
            r zincrby zset -5 bar
            assert_equal {foo bar} [r zrange zset 0 -1]

            assert_equal -2 [r zscore zset foo]
            assert_equal  6 [r zscore zset bar]
        }

        test "ZINCRBY return value" {
            r del ztmp
            set retval [r zincrby ztmp 1.0 x]
            assert {$retval == 1.0}
        }
    }

    basics ziplist

    proc stressers {encoding} {

        set elements 100
        test "ZSCORE - $encoding" {
            r del zscoretest
            set aux {}
            for {set i 0} {$i < $elements} {incr i} {
                set score [expr rand()]
                lappend aux $score
                r zadd zscoretest $score $i
            }

            for {set i 0} {$i < $elements} {incr i} {
                assert_equal [lindex $aux $i] [r zscore zscoretest $i]
            }
        }

        test "ZSET sorting stresser - $encoding" {
            set delta 0
            for {set test 0} {$test < 2} {incr test} {
                unset -nocomplain auxarray
                array set auxarray {}
                set auxlist {}
                r del myzset
                for {set i 0} {$i < $elements} {incr i} {
                    if {$test == 0} {
                        set score [expr rand()]
                    } else {
                        set score [expr int(rand()*10)]
                    }
                    set auxarray($i) $score
                    r zadd myzset $score $i
                    # Random update
                    if {[expr rand()] < .2} {
                        set j [expr int(rand()*1000)]
                        if {$test == 0} {
                            set score [expr rand()]
                        } else {
                            set score [expr int(rand()*10)]
                        }
                        set auxarray($j) $score
                        r zadd myzset $score $j
                    }
                }
                foreach {item score} [array get auxarray] {
                    lappend auxlist [list $score $item]
                }
                set sorted [lsort -command zlistAlikeSort $auxlist]
                set auxlist {}
                foreach x $sorted {
                    lappend auxlist [lindex $x 1]
                }

                set fromredis [r zrange myzset 0 -1]
                set delta 0
                for {set i 0} {$i < [llength $fromredis]} {incr i} {
                    if {[lindex $fromredis $i] != [lindex $auxlist $i]} {
                        incr delta
                    }
                }
            }
            assert_equal 0 $delta
        }

        test "ZSETs skiplist implementation backlink consistency test - $encoding" {
            set diff 0
            for {set j 0} {$j < $elements} {incr j} {
                r zadd myzset [expr rand()] "Element-$j"
                r zrem myzset "Element-[expr int(rand()*$elements)]"
            }

            set l1 [r zrange myzset 0 -1]
            set l2 [r zrevrange myzset 0 -1]
            for {set j 0} {$j < [llength $l1]} {incr j} {
                if {[lindex $l1 $j] ne [lindex $l2 end-$j]} {
                    incr diff
                }
            }
            assert_equal 0 $diff
        }

        test "ZSETs ZRANK augmented skip list stress testing - $encoding" {
            set err {}
            r del myzset
            for {set k 0} {$k < 2000} {incr k} {
                set i [expr {$k % $elements}]
                if {[expr rand()] < .2} {
                    r zrem myzset $i
                } else {
                    set score [expr rand()]
                    r zadd myzset $score $i
                }

                set card [r zcard myzset]
                if {$card > 0} {
                    set index [randomInt $card]
                    set ele [lindex [r zrange myzset $index $index] 0]
                    set rank [r zrank myzset $ele]
                    if {$rank != $index} {
                        set err "$ele RANK is wrong! ($rank != $index)"
                        break
                    }
                }
            }
            assert_equal {} $err
            r del myzset
        }
    }

    tags {"slow"} {
        stressers ziplist
    }
}
