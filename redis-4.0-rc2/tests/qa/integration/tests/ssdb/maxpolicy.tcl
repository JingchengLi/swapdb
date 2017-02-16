start_server {tags {"ssdb"}
overrides {maxmemory-policy {allkeys-lfu} }} {
    set ssdb [redis $::host 8888]
    set redis [redis $::host 6379]
    set usagelimit 0.9

    #currently lfu policy
    #TODO more details about policy
    test "memory efficiency lower than usage limit" {
        $redis config set maxmemory 10000000
        set current_mem [status $redis used_memory]
        set maxmemory [ lindex [ $redis config get maxmemory ] 1 ]
        set efficiency [expr {double($current_mem)/($maxmemory)}]

        set keysNum 100
        set val [string repeat A [ expr { 2*($maxmemory)/$keysNum } ]]
        for {set j 0} {$j < $keysNum} {incr j} {
            $redis set key:$j $val
        }

        while {[expr { [status $redis used_memory] - $current_mem } ] != 0} {
            after 10
            set current_mem [status $redis used_memory]
        }

        set efficiency [expr {double($current_mem)/($maxmemory)}]
        assert { $efficiency < $usagelimit }
        for {set i 0} {$i < $keysNum} {incr i} {
            assert {[$redis exists key:$i] ne 1}
            $redis del key:$i
        }
    }
}
