start_server {tags {"type"}
overrides {maxmemory 0}} {
   proc create_zset {key items} {
        ssdbr del $key
        foreach {score entry} $items {
            ssdbr zadd $key $score $entry
        }
    }

    proc basics {encoding} {
        if {$encoding == "ziplist"} {
            ssdbr config set zset-max-ziplist-entries 128
            ssdbr config set zset-max-ziplist-value 64
        } elseif {$encoding == "skiplist"} {
            ssdbr config set zset-max-ziplist-entries 0
            ssdbr config set zset-max-ziplist-value 0
        } else {
            puts "Unknown sorted set encoding"
            exit
        }

        test "Check encoding - $encoding" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 x
            assert_encoding $encoding ztmp
        }

        test "ZSET basic ZADD and score update - $encoding" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 x
            ssdbr zadd ztmp 20 y
            ssdbr zadd ztmp 30 z
            assert_equal {x y z} [ssdbr zrange ztmp 0 -1]

            ssdbr zadd ztmp 1 y
            assert_equal {y x z} [ssdbr zrange ztmp 0 -1]
        }

        test "ZSET element can't be set to NaN with ZADD - $encoding" {
            assert_error "*not*float*" {r zadd myzset nan abc}
        }

        test "ZSET element can't be set to NaN with ZINCRBY" {
            assert_error "*not*float*" {r zadd myzset nan abc}
        }

        test "ZADD with options syntax error with incomplete pair" {
            ssdbr del ztmp
            catch {r zadd ztmp xx 10 x 20} err
            set err
        } {ERR*}

        test "ZADD XX option without key - $encoding" {
            ssdbr del ztmp
            assert {[ssdbr zadd ztmp xx 10 x] == 0}
            assert {[ssdbr type ztmp] eq {none}}
        }

        test "ZADD XX existing key - $encoding" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 x
            assert {[ssdbr zadd ztmp xx 20 y] == 0}
            assert {[ssdbr zcard ztmp] == 1}
        }

        test "ZADD XX returns the number of elements actually added" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 x
            set retval [ssdbr zadd ztmp 10 x 20 y 30 z]
            assert {$retval == 2}
        }

        test "ZADD XX updates existing elements score" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 x 20 y 30 z
            ssdbr zadd ztmp xx 5 foo 11 x 21 y 40 zap
            assert {[ssdbr zcard ztmp] == 3}
            assert {[ssdbr zscore ztmp x] == 11}
            assert {[ssdbr zscore ztmp y] == 21}
        }

        test "ZADD XX and NX are not compatible" {
            ssdbr del ztmp
            catch {r zadd ztmp xx nx 10 x} err
            set err
        } {ERR*}

        test "ZADD NX with non exisitng key" {
            ssdbr del ztmp
            ssdbr zadd ztmp nx 10 x 20 y 30 z
            assert {[ssdbr zcard ztmp] == 3}
        }

        test "ZADD NX only add new elements without updating old ones" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 x 20 y 30 z
            assert {[ssdbr zadd ztmp nx 11 x 21 y 100 a 200 b] == 2}
            assert {[ssdbr zscore ztmp x] == 10}
            assert {[ssdbr zscore ztmp y] == 20}
            assert {[ssdbr zscore ztmp a] == 100}
            assert {[ssdbr zscore ztmp b] == 200}
        }

        test "ZADD INCR works like ZINCRBY" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 x 20 y 30 z
            ssdbr zadd ztmp INCR 15 x
            assert {[ssdbr zscore ztmp x] == 25}
        }

        test "ZADD INCR works with a single score-elemenet pair" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 x 20 y 30 z
            catch {r zadd ztmp INCR 15 x 10 y} err
            set err
        } {ERR*}

        test "ZADD CH option changes return value to all changed elements" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 x 20 y 30 z
            assert {[ssdbr zadd ztmp 11 x 21 y 30 z] == 0}
            assert {[ssdbr zadd ztmp ch 12 x 22 y 30 z] == 2}
        }

        test "ZINCRBY calls leading to NaN result in error" {
            ssdbr zincrby myzset +inf abc
            assert_error "*NaN*" {ssdbr zincrby myzset -inf abc}
        }

        test {ZADD - Variadic version base case} {
            ssdbr del myzset
            list [ssdbr zadd myzset 10 a 20 b 30 c] [ssdbr zrange myzset 0 -1 withscores]
        } {3 {a 10 b 20 c 30}}

        test {ZADD - Return value is the number of actually added items} {
            list [ssdbr zadd myzset 5 x 20 b 30 c] [ssdbr zrange myzset 0 -1 withscores]
        } {1 {x 5 a 10 b 20 c 30}}

        test {ZADD - Variadic version does not add nothing on single parsing err} {
            ssdbr del myzset
            catch {r zadd myzset 10 a 20 b 30.badscore c} e
            assert_match {*ERR*not*float*} $e
            ssdbr exists myzset
        } {0}

        test {ZADD - Variadic version will raise error on missing arg} {
            ssdbr del myzset
            catch {r zadd myzset 10 a 20 b 30 c 40} e
            assert_match {*ERR*syntax*} $e
        }

        test {ZINCRBY does not work variadic even if shares ZADD implementation} {
            ssdbr del myzset
            catch {r zincrby myzset 10 a 20 b 30 c} e
            assert_match {*ERR*wrong*number*arg*} $e
        }

        test "ZCARD basics - $encoding" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 a 20 b 30 c
            assert_equal 3 [ssdbr zcard ztmp]
            assert_equal 0 [ssdbr zcard zdoesntexist]
        }

        test "ZREM removes key after last element is removed" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 x
            ssdbr zadd ztmp 20 y

            assert_equal 1 [ssdbr exists ztmp]
            assert_equal 0 [ssdbr zrem ztmp z]
            assert_equal 1 [ssdbr zrem ztmp y]
            assert_equal 1 [ssdbr zrem ztmp x]
            assert_equal 0 [ssdbr exists ztmp]
        }

        test "ZREM variadic version" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 a 20 b 30 c
            assert_equal 2 [ssdbr zrem ztmp x y a b k]
            assert_equal 0 [ssdbr zrem ztmp foo bar]
            assert_equal 1 [ssdbr zrem ztmp c]
            ssdbr exists ztmp
        } {0}

        test "ZREM variadic version -- remove elements after key deletion" {
            ssdbr del ztmp
            ssdbr zadd ztmp 10 a 20 b 30 c
            ssdbr zrem ztmp a b c d e f g
        } {3}

        test "ZRANGE basics - $encoding" {
            ssdbr del ztmp
            ssdbr zadd ztmp 1 a
            ssdbr zadd ztmp 2 b
            ssdbr zadd ztmp 3 c
            ssdbr zadd ztmp 4 d

            assert_equal {a b c d} [ssdbr zrange ztmp 0 -1]
            assert_equal {a b c} [ssdbr zrange ztmp 0 -2]
            assert_equal {b c d} [ssdbr zrange ztmp 1 -1]
            assert_equal {b c} [ssdbr zrange ztmp 1 -2]
            assert_equal {c d} [ssdbr zrange ztmp -2 -1]
            assert_equal {c} [ssdbr zrange ztmp -2 -2]

            # out of range start index
            assert_equal {a b c} [ssdbr zrange ztmp -5 2]
            assert_equal {a b} [ssdbr zrange ztmp -5 1]
            assert_equal {} [ssdbr zrange ztmp 5 -1]
            assert_equal {} [ssdbr zrange ztmp 5 -2]

            # out of range end index
            assert_equal {a b c d} [ssdbr zrange ztmp 0 5]
            assert_equal {b c d} [ssdbr zrange ztmp 1 5]
            assert_equal {} [ssdbr zrange ztmp 0 -5]
            assert_equal {} [ssdbr zrange ztmp 1 -5]

            # withscores
            assert_equal {a 1 b 2 c 3 d 4} [ssdbr zrange ztmp 0 -1 withscores]
        }

        test "ZREVRANGE basics - $encoding" {
            ssdbr del ztmp
            ssdbr zadd ztmp 1 a
            ssdbr zadd ztmp 2 b
            ssdbr zadd ztmp 3 c
            ssdbr zadd ztmp 4 d

            assert_equal {d c b a} [ssdbr zrevrange ztmp 0 -1]
            assert_equal {d c b} [ssdbr zrevrange ztmp 0 -2]
            assert_equal {c b a} [ssdbr zrevrange ztmp 1 -1]
            assert_equal {c b} [ssdbr zrevrange ztmp 1 -2]
            assert_equal {b a} [ssdbr zrevrange ztmp -2 -1]
            assert_equal {b} [ssdbr zrevrange ztmp -2 -2]

            # out of range start index
            assert_equal {d c b} [ssdbr zrevrange ztmp -5 2]
            assert_equal {d c} [ssdbr zrevrange ztmp -5 1]
            assert_equal {} [ssdbr zrevrange ztmp 5 -1]
            assert_equal {} [ssdbr zrevrange ztmp 5 -2]

            # out of range end index
            assert_equal {d c b a} [ssdbr zrevrange ztmp 0 5]
            assert_equal {c b a} [ssdbr zrevrange ztmp 1 5]
            assert_equal {} [ssdbr zrevrange ztmp 0 -5]
            assert_equal {} [ssdbr zrevrange ztmp 1 -5]

            # withscores
            assert_equal {d 4 c 3 b 2 a 1} [ssdbr zrevrange ztmp 0 -1 withscores]
        }

        test "ZRANK/ZREVRANK basics - $encoding" {
            ssdbr del zranktmp
            ssdbr zadd zranktmp 10 x
            ssdbr zadd zranktmp 20 y
            ssdbr zadd zranktmp 30 z
            assert_equal 0 [ssdbr zrank zranktmp x]
            assert_equal 1 [ssdbr zrank zranktmp y]
            assert_equal 2 [ssdbr zrank zranktmp z]
            assert_equal "" [ssdbr zrank zranktmp foo]
            assert_equal 2 [ssdbr zrevrank zranktmp x]
            assert_equal 1 [ssdbr zrevrank zranktmp y]
            assert_equal 0 [ssdbr zrevrank zranktmp z]
            assert_equal "" [ssdbr zrevrank zranktmp foo]
        }

        test "ZRANK - after deletion - $encoding" {
            ssdbr zrem zranktmp y
            assert_equal 0 [ssdbr zrank zranktmp x]
            assert_equal 1 [ssdbr zrank zranktmp z]
        }

        test "ZINCRBY - can create a new sorted set - $encoding" {
            ssdbr del zset
            ssdbr zincrby zset 1 foo
            assert_equal {foo} [ssdbr zrange zset 0 -1]
            assert_equal 1 [ssdbr zscore zset foo]
        }

        test "ZINCRBY - increment and decrement - $encoding" {
            ssdbr zincrby zset 2 foo
            ssdbr zincrby zset 1 bar
            assert_equal {bar foo} [ssdbr zrange zset 0 -1]

            ssdbr zincrby zset 10 bar
            ssdbr zincrby zset -5 foo
            ssdbr zincrby zset -5 bar
            assert_equal {foo bar} [ssdbr zrange zset 0 -1]

            assert_equal -2 [ssdbr zscore zset foo]
            assert_equal  6 [ssdbr zscore zset bar]
        }

        test "ZINCRBY return value" {
            ssdbr del ztmp
            set retval [ssdbr zincrby ztmp 1.0 x]
            assert {$retval == 1.0}
        }

        proc create_default_zset {} {
            create_zset zset {-inf a 1 b 2 c 3 d 4 e 5 f +inf g}
        }

        test "ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZCOUNT basics" {
            create_default_zset

            # inclusive range
            assert_equal {a b c} [ssdbr zrangebyscore zset -inf 2]
            assert_equal {b c d} [ssdbr zrangebyscore zset 0 3]
            assert_equal {d e f} [ssdbr zrangebyscore zset 3 6]
            assert_equal {e f g} [ssdbr zrangebyscore zset 4 +inf]
            assert_equal {c b a} [ssdbr zrevrangebyscore zset 2 -inf]
            assert_equal {d c b} [ssdbr zrevrangebyscore zset 3 0]
            assert_equal {f e d} [ssdbr zrevrangebyscore zset 6 3]
            assert_equal {g f e} [ssdbr zrevrangebyscore zset +inf 4]
            assert_equal 3 [ssdbr zcount zset 0 3]

            # exclusive range
            assert_equal {b}   [ssdbr zrangebyscore zset (-inf (2]
            assert_equal {b c} [ssdbr zrangebyscore zset (0 (3]
            assert_equal {e f} [ssdbr zrangebyscore zset (3 (6]
            assert_equal {f}   [ssdbr zrangebyscore zset (4 (+inf]
            assert_equal {b}   [ssdbr zrevrangebyscore zset (2 (-inf]
            assert_equal {c b} [ssdbr zrevrangebyscore zset (3 (0]
            assert_equal {f e} [ssdbr zrevrangebyscore zset (6 (3]
            assert_equal {f}   [ssdbr zrevrangebyscore zset (+inf (4]
            assert_equal 2 [ssdbr zcount zset (0 (3]

            # test empty ranges
            ssdbr zrem zset a
            ssdbr zrem zset g

            # inclusive
            assert_equal {} [ssdbr zrangebyscore zset 4 2]
            assert_equal {} [ssdbr zrangebyscore zset 6 +inf]
            assert_equal {} [ssdbr zrangebyscore zset -inf -6]
            assert_equal {} [ssdbr zrevrangebyscore zset +inf 6]
            assert_equal {} [ssdbr zrevrangebyscore zset -6 -inf]

            # exclusive
            assert_equal {} [ssdbr zrangebyscore zset (4 (2]
            assert_equal {} [ssdbr zrangebyscore zset 2 (2]
            assert_equal {} [ssdbr zrangebyscore zset (2 2]
            assert_equal {} [ssdbr zrangebyscore zset (6 (+inf]
            assert_equal {} [ssdbr zrangebyscore zset (-inf (-6]
            assert_equal {} [ssdbr zrevrangebyscore zset (+inf (6]
            assert_equal {} [ssdbr zrevrangebyscore zset (-6 (-inf]

            # empty inner range
            assert_equal {} [ssdbr zrangebyscore zset 2.4 2.6]
            assert_equal {} [ssdbr zrangebyscore zset (2.4 2.6]
            assert_equal {} [ssdbr zrangebyscore zset 2.4 (2.6]
            assert_equal {} [ssdbr zrangebyscore zset (2.4 (2.6]
        }

        test "ZRANGEBYSCORE with WITHSCORES" {
            create_default_zset
            assert_equal {b 1 c 2 d 3} [ssdbr zrangebyscore zset 0 3 withscores]
            assert_equal {d 3 c 2 b 1} [ssdbr zrevrangebyscore zset 3 0 withscores]
        }

        test "ZRANGEBYSCORE with LIMIT" {
            create_default_zset
            assert_equal {b c}   [ssdbr zrangebyscore zset 0 10 LIMIT 0 2]
            assert_equal {d e f} [ssdbr zrangebyscore zset 0 10 LIMIT 2 3]
            assert_equal {d e f} [ssdbr zrangebyscore zset 0 10 LIMIT 2 10]
            assert_equal {}      [ssdbr zrangebyscore zset 0 10 LIMIT 20 10]
            assert_equal {f e}   [ssdbr zrevrangebyscore zset 10 0 LIMIT 0 2]
            assert_equal {d c b} [ssdbr zrevrangebyscore zset 10 0 LIMIT 2 3]
            assert_equal {d c b} [ssdbr zrevrangebyscore zset 10 0 LIMIT 2 10]
            assert_equal {}      [ssdbr zrevrangebyscore zset 10 0 LIMIT 20 10]
        }

        test "ZRANGEBYSCORE with LIMIT and WITHSCORES" {
            create_default_zset
            assert_equal {e 4 f 5} [ssdbr zrangebyscore zset 2 5 LIMIT 2 3 WITHSCORES]
            assert_equal {d 3 c 2} [ssdbr zrevrangebyscore zset 5 2 LIMIT 2 3 WITHSCORES]
        }

        test "ZRANGEBYSCORE with non-value min or max" {
            assert_error "*not*float*" {r zrangebyscore fooz str 1}
            assert_error "*not*float*" {r zrangebyscore fooz 1 str}
            assert_error "*not*float*" {r zrangebyscore fooz 1 NaN}
        }

        proc create_default_lex_zset {} {
            create_zset zset {0 alpha 0 bar 0 cool 0 down
                              0 elephant 0 foo 0 great 0 hill
                              0 omega}
        }

        test "ZRANGEBYLEX/ZREVRANGEBYLEX/ZCOUNT basics" {
            create_default_lex_zset

            # inclusive range
            assert_equal {alpha bar cool} [ssdbr zrangebylex zset - \[cool]
            assert_equal {bar cool down} [ssdbr zrangebylex zset \[bar \[down]
            assert_equal {great hill omega} [ssdbr zrangebylex zset \[g +]
            assert_equal {cool bar alpha} [ssdbr zrevrangebylex zset \[cool -]
            assert_equal {down cool bar} [ssdbr zrevrangebylex zset \[down \[bar]
            assert_equal {omega hill great foo elephant down} [ssdbr zrevrangebylex zset + \[d]
            assert_equal 3 [ssdbr zlexcount zset \[ele \[h]

            # exclusive range
            assert_equal {alpha bar} [ssdbr zrangebylex zset - (cool]
            assert_equal {cool} [ssdbr zrangebylex zset (bar (down]
            assert_equal {hill omega} [ssdbr zrangebylex zset (great +]
            assert_equal {bar alpha} [ssdbr zrevrangebylex zset (cool -]
            assert_equal {cool} [ssdbr zrevrangebylex zset (down (bar]
            assert_equal {omega hill} [ssdbr zrevrangebylex zset + (great]
            assert_equal 2 [ssdbr zlexcount zset (ele (great]

            # inclusive and exclusive
            assert_equal {} [ssdbr zrangebylex zset (az (b]
            assert_equal {} [ssdbr zrangebylex zset (z +]
            assert_equal {} [ssdbr zrangebylex zset - \[aaaa]
            assert_equal {} [ssdbr zrevrangebylex zset \[elez \[elex]
            assert_equal {} [ssdbr zrevrangebylex zset (hill (omega]
        }

        test "ZRANGEBYSLEX with LIMIT" {
            create_default_lex_zset
            assert_equal {alpha bar} [ssdbr zrangebylex zset - \[cool LIMIT 0 2]
            assert_equal {bar cool} [ssdbr zrangebylex zset - \[cool LIMIT 1 2]
            assert_equal {} [ssdbr zrangebylex zset \[bar \[down LIMIT 0 0]
            assert_equal {} [ssdbr zrangebylex zset \[bar \[down LIMIT 2 0]
            assert_equal {bar} [ssdbr zrangebylex zset \[bar \[down LIMIT 0 1]
            assert_equal {cool} [ssdbr zrangebylex zset \[bar \[down LIMIT 1 1]
            assert_equal {bar cool down} [ssdbr zrangebylex zset \[bar \[down LIMIT 0 100]
            assert_equal {omega hill great foo elephant} [ssdbr zrevrangebylex zset + \[d LIMIT 0 5]
            assert_equal {omega hill great foo} [ssdbr zrevrangebylex zset + \[d LIMIT 0 4]
        }

        test "ZRANGEBYLEX with invalid lex range specifiers" {
            assert_error "*not*string*" {r zrangebylex fooz foo bar}
            assert_error "*not*string*" {r zrangebylex fooz \[foo bar}
            assert_error "*not*string*" {r zrangebylex fooz foo \[bar}
            assert_error "*not*string*" {r zrangebylex fooz +x \[bar}
            assert_error "*not*string*" {r zrangebylex fooz -x \[bar}
        }

        test "ZREMRANGEBYSCORE basics" {
            proc remrangebyscore {min max} {
                create_zset zset {1 a 2 b 3 c 4 d 5 e}
                assert_equal 1 [ssdbr exists zset]
                ssdbr zremrangebyscore zset $min $max
            }

            # inner range
            assert_equal 3 [remrangebyscore 2 4]
            assert_equal {a e} [ssdbr zrange zset 0 -1]

            # start underflow
            assert_equal 1 [remrangebyscore -10 1]
            assert_equal {b c d e} [ssdbr zrange zset 0 -1]

            # end overflow
            assert_equal 1 [remrangebyscore 5 10]
            assert_equal {a b c d} [ssdbr zrange zset 0 -1]

            # switch min and max
            assert_equal 0 [remrangebyscore 4 2]
            assert_equal {a b c d e} [ssdbr zrange zset 0 -1]

            # -inf to mid
            assert_equal 3 [remrangebyscore -inf 3]
            assert_equal {d e} [ssdbr zrange zset 0 -1]

            # mid to +inf
            assert_equal 3 [remrangebyscore 3 +inf]
            assert_equal {a b} [ssdbr zrange zset 0 -1]

            # -inf to +inf
            assert_equal 5 [remrangebyscore -inf +inf]
            assert_equal {} [ssdbr zrange zset 0 -1]

            # exclusive min
            assert_equal 4 [remrangebyscore (1 5]
            assert_equal {a} [ssdbr zrange zset 0 -1]
            assert_equal 3 [remrangebyscore (2 5]
            assert_equal {a b} [ssdbr zrange zset 0 -1]

            # exclusive max
            assert_equal 4 [remrangebyscore 1 (5]
            assert_equal {e} [ssdbr zrange zset 0 -1]
            assert_equal 3 [remrangebyscore 1 (4]
            assert_equal {d e} [ssdbr zrange zset 0 -1]

            # exclusive min and max
            assert_equal 3 [remrangebyscore (1 (5]
            assert_equal {a e} [ssdbr zrange zset 0 -1]

            # destroy when empty
            assert_equal 5 [remrangebyscore 1 5]
            assert_equal 0 [ssdbr exists zset]
        }

        test "ZREMRANGEBYSCORE with non-value min or max" {
            assert_error "*not*float*" {r zremrangebyscore fooz str 1}
            assert_error "*not*float*" {r zremrangebyscore fooz 1 str}
            assert_error "*not*float*" {r zremrangebyscore fooz 1 NaN}
        }

        test "ZREMRANGEBYRANK basics" {
            proc remrangebyrank {min max} {
                create_zset zset {1 a 2 b 3 c 4 d 5 e}
                assert_equal 1 [ssdbr exists zset]
                ssdbr zremrangebyrank zset $min $max
            }

            # inner range
            assert_equal 3 [remrangebyrank 1 3]
            assert_equal {a e} [ssdbr zrange zset 0 -1]

            # start underflow
            assert_equal 1 [remrangebyrank -10 0]
            assert_equal {b c d e} [ssdbr zrange zset 0 -1]

            # start overflow
            assert_equal 0 [remrangebyrank 10 -1]
            assert_equal {a b c d e} [ssdbr zrange zset 0 -1]

            # end underflow
            assert_equal 0 [remrangebyrank 0 -10]
            assert_equal {a b c d e} [ssdbr zrange zset 0 -1]

            # end overflow
            assert_equal 5 [remrangebyrank 0 10]
            assert_equal {} [ssdbr zrange zset 0 -1]

            # destroy when empty
            assert_equal 5 [remrangebyrank 0 4]
            assert_equal 0 [ssdbr exists zset]
        }

#        test "ZUNIONSTORE against non-existing key doesn't set destination - $encoding" {
#            ssdbr del zseta
#            assert_equal 0 [ssdbr zunionstore dst_key 1 zseta]
#            assert_equal 0 [ssdbr exists dst_key]
#        }
#
#        test "ZUNIONSTORE with empty set - $encoding" {
#            ssdbr del zseta zsetb
#            ssdbr zadd zseta 1 a
#            ssdbr zadd zseta 2 b
#            ssdbr zunionstore zsetc 2 zseta zsetb
#            ssdbr zrange zsetc 0 -1 withscores
#        } {a 1 b 2}
#
#        test "ZUNIONSTORE basics - $encoding" {
#            ssdbr del zseta zsetb zsetc
#            ssdbr zadd zseta 1 a
#            ssdbr zadd zseta 2 b
#            ssdbr zadd zseta 3 c
#            ssdbr zadd zsetb 1 b
#            ssdbr zadd zsetb 2 c
#            ssdbr zadd zsetb 3 d
#
#            assert_equal 4 [ssdbr zunionstore zsetc 2 zseta zsetb]
#            assert_equal {a 1 b 3 d 3 c 5} [ssdbr zrange zsetc 0 -1 withscores]
#        }
#
#        test "ZUNIONSTORE with weights - $encoding" {
#            assert_equal 4 [ssdbr zunionstore zsetc 2 zseta zsetb weights 2 3]
#            assert_equal {a 2 b 7 d 9 c 12} [ssdbr zrange zsetc 0 -1 withscores]
#        }
#
#        test "ZUNIONSTORE with a regular set and weights - $encoding" {
#            ssdbr del seta
#            ssdbr sadd seta a
#            ssdbr sadd seta b
#            ssdbr sadd seta c
#
#            assert_equal 4 [ssdbr zunionstore zsetc 2 seta zsetb weights 2 3]
#            assert_equal {a 2 b 5 c 8 d 9} [ssdbr zrange zsetc 0 -1 withscores]
#        }
#
#        test "ZUNIONSTORE with AGGREGATE MIN - $encoding" {
#            assert_equal 4 [ssdbr zunionstore zsetc 2 zseta zsetb aggregate min]
#            assert_equal {a 1 b 1 c 2 d 3} [ssdbr zrange zsetc 0 -1 withscores]
#        }
#
#        test "ZUNIONSTORE with AGGREGATE MAX - $encoding" {
#            assert_equal 4 [ssdbr zunionstore zsetc 2 zseta zsetb aggregate max]
#            assert_equal {a 1 b 2 c 3 d 3} [ssdbr zrange zsetc 0 -1 withscores]
#        }
#
#        test "ZINTERSTORE basics - $encoding" {
#            assert_equal 2 [ssdbr zinterstore zsetc 2 zseta zsetb]
#            assert_equal {b 3 c 5} [ssdbr zrange zsetc 0 -1 withscores]
#        }
#
#        test "ZINTERSTORE with weights - $encoding" {
#            assert_equal 2 [ssdbr zinterstore zsetc 2 zseta zsetb weights 2 3]
#            assert_equal {b 7 c 12} [ssdbr zrange zsetc 0 -1 withscores]
#        }
#
#        test "ZINTERSTORE with a regular set and weights - $encoding" {
#            ssdbr del seta
#            ssdbr sadd seta a
#            ssdbr sadd seta b
#            ssdbr sadd seta c
#            assert_equal 2 [ssdbr zinterstore zsetc 2 seta zsetb weights 2 3]
#            assert_equal {b 5 c 8} [ssdbr zrange zsetc 0 -1 withscores]
#        }
#
#        test "ZINTERSTORE with AGGREGATE MIN - $encoding" {
#            assert_equal 2 [ssdbr zinterstore zsetc 2 zseta zsetb aggregate min]
#            assert_equal {b 1 c 2} [ssdbr zrange zsetc 0 -1 withscores]
#        }
#
#        test "ZINTERSTORE with AGGREGATE MAX - $encoding" {
#            assert_equal 2 [ssdbr zinterstore zsetc 2 zseta zsetb aggregate max]
#            assert_equal {b 2 c 3} [ssdbr zrange zsetc 0 -1 withscores]
#        }
    }

    basics ziplist
    basics skiplist

#    test {ZINTERSTORE regression with two sets, intset+hashtable} {
#        ssdbr del seta setb setc
#        ssdbr sadd set1 a
#        ssdbr sadd set2 10
#        ssdbr zinterstore set3 2 set1 set2
#    } {0}
#
#    test {ZUNIONSTORE regression, should not create NaN in scores} {
#        ssdbr zadd z -inf neginf
#        ssdbr zunionstore out 1 z weights 0
#        ssdbr zrange out 0 -1 withscores
#    } {neginf 0}
#
#    test {ZINTERSTORE #516 regression, mixed sets and ziplist zsets} {
#        ssdbr sadd one 100 101 102 103
#        ssdbr sadd two 100 200 201 202
#        ssdbr zadd three 1 500 1 501 1 502 1 503 1 100
#        ssdbr zinterstore to_here 3 one two three WEIGHTS 0 0 1
#        ssdbr zrange to_here 0 -1
#    } {100}
#
#    test {ZUNIONSTORE result is sorted} {
#        # Create two sets with common and not common elements, perform
#        # the UNION, check that elements are still sorted.
#        ssdbr del one two dest
#        set cmd1 [list r zadd one]
#        set cmd2 [list r zadd two]
#        for {set j 0} {$j < 1000} {incr j} {
#            lappend cmd1 [expr rand()] [randomInt 1000]
#            lappend cmd2 [expr rand()] [randomInt 1000]
#        }
#        {*}$cmd1
#        {*}$cmd2
#        assert {[ssdbr zcard one] > 100}
#        assert {[ssdbr zcard two] > 100}
#        ssdbr zunionstore dest 2 one two
#        set oldscore 0
#        foreach {ele score} [ssdbr zrange dest 0 -1 withscores] {
#            assert {$score >= $oldscore}
#            set oldscore $score
#        }
#    }

    proc stressers {encoding} {
        if {$encoding == "ziplist"} {
            # Little extra to allow proper fuzzing in the sorting stresser
            ssdbr config set zset-max-ziplist-entries 256
            ssdbr config set zset-max-ziplist-value 64
            set elements 128
        } elseif {$encoding == "skiplist"} {
            ssdbr config set zset-max-ziplist-entries 0
            ssdbr config set zset-max-ziplist-value 0
            if {$::accurate} {set elements 1000} else {set elements 50}
        } else {
            puts "Unknown sorted set encoding"
            exit
        }

        test "ZSCORE INT - $encoding" {
            ssdbr del zscoretest
            set aux {}
            for {set i 0} {$i < $elements} {incr i} {
                set score [randomSignedInt 999999999]
                lappend aux $score
                ssdbr zadd zscoretest $score $i
            }

            assert_encoding $encoding zscoretest
            for {set i 0} {$i < $elements} {incr i} {
                assert_equal [lindex $aux $i] [ssdbr zscore zscoretest $i]
            }
        }

        set eps 0.00001
        test "ZSCORE DECIMAL - $encoding" {
            ssdbr del zscoretest
            set aux {}
            for {set i 0} {$i < $elements} {incr i} {
                set score [expr rand()]
                lappend aux $score
                ssdbr zadd zscoretest $score $i
            }

            assert_encoding $encoding zscoretest
            for {set i 0} {$i < $elements} {incr i} {
                assert { abs([expr [lindex $aux $i]-[ssdbr zscore zscoretest $i]]) < $eps  }
            }
        }

        test "ZSCORE INT after a DEBUG RELOAD - $encoding" {
            ssdbr del zscoretest
            set aux {}
            for {set i 0} {$i < $elements} {incr i} {
                set score [randomSignedInt 9999999]
                lappend aux $score
                ssdbr zadd zscoretest $score $i
            }

            ssdbr debug reload
            assert_encoding $encoding zscoretest
            for {set i 0} {$i < $elements} {incr i} {
                assert_equal [lindex $aux $i] [ssdbr zscore zscoretest $i]
            }
        }

        test "ZSCORE DECIMAL after a DEBUG RELOAD - $encoding" {
            ssdbr del zscoretest
            set aux {}
            for {set i 0} {$i < $elements} {incr i} {
                set score [expr rand()]
                lappend aux $score
                ssdbr zadd zscoretest $score $i
            }

            ssdbr debug reload
            assert_encoding $encoding zscoretest
            for {set i 0} {$i < $elements} {incr i} {
                assert_equal [lindex $aux $i] [ssdbr zscore zscoretest $i]
            }
        }

        test "ZSET sorting stresser - $encoding" {
            set delta 0
            for {set test 0} {$test < 2} {incr test} {
                unset -nocomplain auxarray
                array set auxarray {}
                set auxlist {}
                ssdbr del myzset
                for {set i 0} {$i < $elements} {incr i} {
                    if {$test == 0} {
                        set score [expr rand()]
                    } else {
                        set score [expr int(rand()*10)]
                    }
                    set auxarray($i) $score
                    ssdbr zadd myzset $score $i
                    # Random update
                    if {[expr rand()] < .2} {
                        set j [expr int(rand()*1000)]
                        if {$test == 0} {
                            set score [expr rand()]
                        } else {
                            set score [expr int(rand()*10)]
                        }
                        set auxarray($j) $score
                        ssdbr zadd myzset $score $j
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

                assert_encoding $encoding myzset
                set fromredis [ssdbr zrange myzset 0 -1]
                set delta 0
                for {set i 0} {$i < [llength $fromredis]} {incr i} {
                    if {[lindex $fromredis $i] != [lindex $auxlist $i]} {
                        incr delta
                    }
                }
            }
            assert_equal 0 $delta
        }

        test "ZRANGEBYSCORE fuzzy test, 100 ranges in $elements element sorted set - $encoding" {
            set err {}
            ssdbr del zset
            for {set i 0} {$i < $elements} {incr i} {
                ssdbr zadd zset [expr rand()] $i
            }

            assert_encoding $encoding zset
            if {$::accurate} {
                set num 100
            } else {
                set num 1
            }
            for {set i 0} {$i < $num} {incr i} {
                set min [expr rand()]
                set max [expr rand()]
                if {$min > $max} {
                    set aux $min
                    set min $max
                    set max $aux
                }
                set low [ssdbr zrangebyscore zset -inf $min]
                set ok [ssdbr zrangebyscore zset $min $max]
                set high [ssdbr zrangebyscore zset $max +inf]
                set lowx [ssdbr zrangebyscore zset -inf ($min]
                set okx [ssdbr zrangebyscore zset ($min ($max]
                set highx [ssdbr zrangebyscore zset ($max +inf]

                if {[ssdbr zcount zset -inf $min] != [llength $low]} {
                    append err "Error, len does not match zcount\n"
                }
                if {[ssdbr zcount zset $min $max] != [llength $ok]} {
                    append err "Error, len does not match zcount\n"
                }
                if {[ssdbr zcount zset $max +inf] != [llength $high]} {
                    append err "Error, len does not match zcount\n"
                }
                if {[ssdbr zcount zset -inf ($min] != [llength $lowx]} {
                    append err "Error, len does not match zcount\n"
                }
                if {[ssdbr zcount zset ($min ($max] != [llength $okx]} {
                    append err "Error, len does not match zcount\n"
                }
                if {[ssdbr zcount zset ($max +inf] != [llength $highx]} {
                    append err "Error, len does not match zcount\n"
                }

                foreach x $low {
                    set score [ssdbr zscore zset $x]
                    if {$score > $min} {
                        append err "Error, score for $x is $score > $min\n"
                    }
                }
                foreach x $lowx {
                    set score [ssdbr zscore zset $x]
                    if {$score >= $min} {
                        append err "Error, score for $x is $score >= $min\n"
                    }
                }
                foreach x $ok {
                    set score [ssdbr zscore zset $x]
                    if {$score < $min || $score > $max} {
                        append err "Error, score for $x is $score outside $min-$max range\n"
                    }
                }
                foreach x $okx {
                    set score [ssdbr zscore zset $x]
                    if {$score <= $min || $score >= $max} {
                        append err "Error, score for $x is $score outside $min-$max open range\n"
                    }
                }
                foreach x $high {
                    set score [ssdbr zscore zset $x]
                    if {$score < $max} {
                        append err "Error, score for $x is $score < $max\n"
                    }
                }
                foreach x $highx {
                    set score [ssdbr zscore zset $x]
                    if {$score <= $max} {
                        append err "Error, score for $x is $score <= $max\n"
                    }
                }
            }
            assert_equal {} $err
        }

        test "ZRANGEBYLEX fuzzy test, 100 ranges in $elements element sorted set - $encoding" {
            set lexset {}
            ssdbr del zset
            for {set j 0} {$j < $elements} {incr j} {
                set e [randstring 0 30 alpha]
                lappend lexset $e
                ssdbr zadd zset 0 $e
            }
            set lexset [lsort -unique $lexset]
            if {$::accurate} {
                set num 100
            } else {
                set num 2
            }
            for {set j 0} {$j < $num} {incr j} {
                set min [randstring 0 30 alpha]
                set max [randstring 0 30 alpha]
                set mininc [randomInt 2]
                set maxinc [randomInt 2]
                if {$mininc} {set cmin "\[$min"} else {set cmin "($min"}
                if {$maxinc} {set cmax "\[$max"} else {set cmax "($max"}
                set rev [randomInt 2]
                if {$rev} {
                    set cmd zrevrangebylex
                } else {
                    set cmd zrangebylex
                }

                # Make sure data is the same in both sides
                assert {[ssdbr zrange zset 0 -1] eq $lexset}

                # Get the Redis output
                set output [ssdbr $cmd zset $cmin $cmax]
                if {$rev} {
                    set outlen [ssdbr zlexcount zset $cmax $cmin]
                } else {
                    set outlen [ssdbr zlexcount zset $cmin $cmax]
                }

                # Compute the same output via Tcl
                set o {}
                set copy $lexset
                if {(!$rev && [string compare $min $max] > 0) ||
                    ($rev && [string compare $max $min] > 0)} {
                    # Empty output when ranges are inverted.
                } else {
                    if {$rev} {
                        # Invert the Tcl array using Redis itself.
                        set copy [ssdbr zrevrange zset 0 -1]
                        # Invert min / max as well
                        lassign [list $min $max $mininc $maxinc] \
                            max min maxinc mininc
                    }
                    foreach e $copy {
                        set mincmp [string compare $e $min]
                        set maxcmp [string compare $e $max]
                        if {
                             ($mininc && $mincmp >= 0 || !$mininc && $mincmp > 0)
                             &&
                             ($maxinc && $maxcmp <= 0 || !$maxinc && $maxcmp < 0)
                        } {
                            lappend o $e
                        }
                    }
                }
                assert {$o eq $output}
                assert {$outlen eq [llength $output]}
            }
        }

        test "ZREMRANGEBYLEX fuzzy test, 100 ranges in $elements element sorted set - $encoding" {
            set lexset {}
            ssdbr del zset
            ssdbr del zsetcopy
            # ssdbr del zset zsetcopy
            for {set j 0} {$j < $elements} {incr j} {
                set e [randstring 0 30 alpha]
                lappend lexset $e
                ssdbr zadd zset 0 $e
            }
            set lexset [lsort -unique $lexset]
            if {$::accurate} {
                set num 100
            } else {
                set num 2
            }
            for {set j 0} {$j < $num} {incr j} {
                # Copy...
                r restore zsetcopy 0 [r dump zset] replace
                # ssdbr zunionstore zsetcopy 1 zset
                set lexsetcopy $lexset

                set min [randstring 0 30 alpha]
                set max [randstring 0 30 alpha]
                set mininc [randomInt 2]
                set maxinc [randomInt 2]
                if {$mininc} {set cmin "\[$min"} else {set cmin "($min"}
                if {$maxinc} {set cmax "\[$max"} else {set cmax "($max"}

                # Make sure data is the same in both sides
                assert {[ssdbr zrange zset 0 -1] eq $lexset}

                # Get the range we are going to remove
                set torem [ssdbr zrangebylex zset $cmin $cmax]
                set toremlen [ssdbr zlexcount zset $cmin $cmax]
                ssdbr zremrangebylex zsetcopy $cmin $cmax
                set output [ssdbr zrange zsetcopy 0 -1]

                # Remove the range with Tcl from the original list
                if {$toremlen} {
                    set first [lsearch -exact $lexsetcopy [lindex $torem 0]]
                    set last [expr {$first+$toremlen-1}]
                    set lexsetcopy [lreplace $lexsetcopy $first $last]
                }
                assert {$lexsetcopy eq $output}
            }
        }

        test "ZSETs skiplist implementation backlink consistency test - $encoding" {
            set diff 0
            for {set j 0} {$j < $elements} {incr j} {
                ssdbr zadd myzset [expr rand()] "Element-$j"
                ssdbr zrem myzset "Element-[expr int(rand()*$elements)]"
            }

            assert_encoding $encoding myzset
            set l1 [ssdbr zrange myzset 0 -1]
            set l2 [ssdbr zrevrange myzset 0 -1]
            for {set j 0} {$j < [llength $l1]} {incr j} {
                if {[lindex $l1 $j] ne [lindex $l2 end-$j]} {
                    incr diff
                }
            }
            assert_equal 0 $diff
        }

        test "ZSETs ZRANK augmented skip list stress testing - $encoding" {
            set err {}
            ssdbr del myzset
            if {$::accurate} {
                set loops 500
            } else {
                set loops 50
            }
            for {set k 0} {$k < $loops} {incr k} {
                set i [expr {$k % $elements}]
                if {[expr rand()] < .2} {
                    ssdbr zrem myzset $i
                } else {
                    set score [expr rand()]
                    ssdbr zadd myzset $score $i
                    assert_encoding $encoding myzset
                }

                set card [ssdbr zcard myzset]
                if {$card > 0} {
                    set index [randomInt $card]
                    set ele [lindex [ssdbr zrange myzset $index $index] 0]
                    set rank [ssdbr zrank myzset $ele]
                    if {$rank != $index} {
                        set err "$ele RANK is wrong! ($rank != $index)"
                        break
                    }
                }
            }
            assert_equal {} $err
        }
    }

    tags {"slow"} {
        stressers ziplist
        stressers skiplist
    }
}
