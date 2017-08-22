start_server {tags {"ssdb"}
overrides {maxmemory-policy {allkeys-lfu} }} {
    set ssdb [redis $::host $::ssdbport]
    set usagelimit 0.5

    #currently lfu policy
    #TODO more details about policy
    test "memory efficiency lower than usage limit" {
        r config set maxmemory 10000000
        set current_mem [status r used_memory]
        set maxmemory [ lindex [ r config get maxmemory ] 1 ]
        set efficiency [expr {double($current_mem)/($maxmemory)}]

        set keysNum 100
        set val [string repeat A [ expr { 2*($maxmemory)/$keysNum } ]]
        for {set j 0} {$j < $keysNum} {incr j} {
            r set key:$j $val
        }

        while {[expr { [status r used_memory] - $current_mem } ] != 0} {
            after 10
            set current_mem [status r used_memory]
        }

        set efficiency [expr {double($current_mem)/($maxmemory)}]
        assert { $efficiency < $usagelimit }
        for {set i 0} {$i < $keysNum} {incr i} {
            assert {[r exists key:$i] ne 1}
            r del key:$i
        }
    }
}
