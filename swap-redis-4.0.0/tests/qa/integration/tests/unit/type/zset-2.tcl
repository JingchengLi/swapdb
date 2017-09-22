start_server {tags {"type"}
overrides {maxmemory 0}} {
    test "Exceed MAX/MIN score return err" {
        ssdbr del zscoretest
        assert_error "*valid float*" {ssdbr zadd zscoretest 1e309 x}
        assert_equal 0 [ssdbr exists zscoretest]
        assert_error "*valid float*" {ssdbr zadd zscoretest -1e309 y}
        assert_equal 0 [ssdbr exists zscoretest]
    }

    test "Support double float MAX/MIN score" {
        r del zscoretest
        assert_equal 1 [ssdbr zadd zscoretest 1e308 m]
        assert_equal {1e+308} [ssdbr zscore zscoretest m]
        assert_equal 1 [ssdbr zadd zscoretest -1e308 n]
        assert_equal {-1e+308} [ssdbr zscore zscoretest n]
    }

    test "Support zadd +/-inf key" {
        ssdbr del ztmp
        assert_equal 1 [ssdbr zadd ztmp inf m]
        assert_equal {inf} [ssdbr zscore ztmp m]
        assert_equal 1 [ssdbr zadd ztmp -inf n]
        assert_equal {-inf} [ssdbr zscore ztmp n]
    }

    test "zadd incr +/-inf key" {
        ssdbr del ztmp
        assert_equal {inf} [ssdbr zadd ztmp incr inf m]
        assert_equal {inf} [ssdbr zscore ztmp m]
        assert_equal {-inf} [ssdbr zadd ztmp incr -inf n]
        assert_equal {-inf} [ssdbr zscore ztmp n]
    }

    test "zincrby +/-inf key" {
        ssdbr del ztmp
        assert_equal {inf} [ssdbr zincrby ztmp inf m]
        assert_equal {inf} [ssdbr zscore ztmp m]
        assert_equal {-inf} [ssdbr zincrby ztmp -inf n]
        assert_equal {-inf} [ssdbr zscore ztmp n]
    }

    if {$::accurate} {
        set elements 500
    } else {
        set elements 50
    }
    test "Big score int range" {
        ssdbr del zscoretest
        set aux {}
        for {set i 0} {$i < $elements} {incr i} {
            set score [randomSignedInt 9007199254740992]
            lappend aux $score
            ssdbr zadd zscoretest $score $i
        }

        set flag 0
        for {set i 0; set j 0} {$i < $elements} {incr i} {
            if {[lindex $aux $i] ne [ssdbr zscore zscoretest $i]} {
                set flag 1
                puts "[lindex $aux $i]:[ssdbr zscore zscoretest $i]"
                incr j
                if {$j > 5} {
                    break
                }
            }
        }
        assert_equal 0 $flag "some big int score lose accuracy."
    }

    test "Decimal score accuracy" {
        ssdbr del zscoretest
        set aux {}
        for {set i 0} {$i < $elements} {incr i} {
            set score [expr rand()]
            lappend aux $score
            ssdbr zadd zscoretest $score $i
        }

        set flag 0
        for {set i 0; set j 0} {$i < $elements} {incr i} {
            if {[lindex $aux $i] ne [ssdbr zscore zscoretest $i]} {
                set flag 1
                puts "[lindex $aux $i]:[ssdbr zscore zscoretest $i]"
                incr j
                if {$j > 5} {
                    break
                }
            }
        }
        assert_equal 0 $flag "some decimal score lose accuracy."
    }
}
