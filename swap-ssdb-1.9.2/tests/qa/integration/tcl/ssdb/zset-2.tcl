source "./ssdb/init_ssdb.tcl"
start_server {tags {"zset"}} {
    test "Exceed MAX/MIN score in ssdb return err" {
        r del zscoretest
        assert_error "*valid float*" {r zadd zscoretest 1e309 x}
        assert_equal 0 [r exists zscoretest]
        assert_error "*valid float*" {r zadd zscoretest -1e309 y}
        assert_equal 0 [r exists zscoretest]
    }

    test "Support double float MAX/MIN score in ssdb" {
        r del zscoretest
        assert_equal 1 [r zadd zscoretest 1e308 m]
        assert_equal {1e+308} [r zscore zscoretest m]
        assert_equal 1 [r zadd zscoretest -1e308 n]
        assert_equal {-1e+308} [r zscore zscoretest n]
    }

    test "Support zadd +/-inf key in ssdb" {
        r del ztmp
        assert_equal 1 [r zadd ztmp inf m]
        assert_equal {inf} [r zscore ztmp m]
        assert_equal 1 [r zadd ztmp -inf n]
        assert_equal {-inf} [r zscore ztmp n]
    }

    test "zadd incr +/-inf key" {
        r del ztmp
        assert_equal {inf} [r zadd ztmp incr inf m]
        assert_equal {inf} [r zscore ztmp m]
        assert_equal {-inf} [r zadd ztmp incr -inf n]
        assert_equal {-inf} [r zscore ztmp n]
    }

    test "zincrby +/-inf key" {
        r del ztmp
        assert_equal {inf} [r zincrby ztmp inf m]
        assert_equal {inf} [r zscore ztmp m]
        assert_equal {-inf} [r zincrby ztmp -inf n]
        assert_equal {-inf} [r zscore ztmp n]
    }

    if {$::accurate} {
        set elements 500
    } else {
        set elements 50
    }
    test "Big score int range" {
        r del zscoretest
        set aux {}
        for {set i 0} {$i < $elements} {incr i} {
            set score [randomSignedInt 9007199254740992]
            lappend aux $score
            r zadd zscoretest $score $i
        }

        set flag 0
        for {set i 0; set j 0} {$i < $elements} {incr i} {
            if {[lindex $aux $i] ne [r zscore zscoretest $i]} {
                set flag 1
                puts "[lindex $aux $i]:[r zscore zscoretest $i]"
                incr j
                if {$j > 5} {
                    break
                }
            }
        }
        assert_equal 0 $flag "some big int score lose accuracy."
    }

    test "Decimal score accuracy" {
        r del zscoretest
        set aux {}
        for {set i 0} {$i < $elements} {incr i} {
            set score [expr rand()]
            lappend aux $score
            r zadd zscoretest $score $i
        }

        set flag 0
        for {set i 0; set j 0} {$i < $elements} {incr i} {
            if {[lindex $aux $i] ne [r zscore zscoretest $i]} {
                set flag 1
                puts "[lindex $aux $i]:[r zscore zscoretest $i]"
                incr j
                if {$j > 5} {
                    break
                }
            }
        }
        assert_equal 0 $flag "some decimal score lose accuracy."
    }
}
