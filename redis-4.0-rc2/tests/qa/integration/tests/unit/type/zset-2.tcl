start_server {tags {"type"}
overrides {maxmemory 0}} {
    set elements 500
    test "Big score int range +/- 2.8e12" {
    #   127.0.0.1:41212>  zadd zk 3021985275215 q
    #   (integer) 1
    #   127.0.0.1:41212> dump zk
    #   "\x03\x01\x01q\x123021985275215.0005\b\x00\xfa\x0c3wK!s\x0e"
        ssdbr del zscoretest
        set aux {}
        for {set i 0} {$i < $elements} {incr i} {
            set score [randomSignedInt 2800000000000]
            lappend aux $score
            ssdbr zadd zscoretest $score $i
        }

        set flag 0
        for {set i 0; set j 0} {$i < $elements} {incr i} {
            if {[lindex $aux $i] ne [ssdbr zscore zscoretest $i]} {
                set flag 1
                puts "[lindex $aux $i]:[ssdbr zscore zscoretest $i]"
                incr j
                if {$j > 15} {
                    break
                }
            }
        }
        assert_equal 0 $flag "some big int score lose accuracy."
    }

    set eps 0.00001
    test "Decimal score accuracy 5 bits" {
        ssdbr del zscoretest
        set aux {}
        for {set i 0} {$i < $elements} {incr i} {
            set score [expr rand()]
            lappend aux $score
            ssdbr zadd zscoretest $score $i
        }

        for {set i 0} {$i < $elements} {incr i} {
            assert { abs([expr [lindex $aux $i]-[ssdbr zscore zscoretest $i]]) < $eps  }
        }

        set flag 0
        for {set i 0; set j 0} {$i < $elements} {incr i} {
            if {abs([expr [lindex $aux $i]-[ssdbr zscore zscoretest $i]]) > $eps} {
                set flag 1
                puts "[lindex $aux $i]:[ssdbr zscore zscoretest $i]"
                incr j
                if {$j > 15} {
                    break
                }
            }
        }
        assert_equal 0 $flag "some decimal score lose accuracy."
    }
}
