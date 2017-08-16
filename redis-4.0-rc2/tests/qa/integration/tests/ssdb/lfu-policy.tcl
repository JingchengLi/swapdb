start_server {tags {"lfu"}} {
    test "Maxmemory-policy default noeviction at startup when no set in config file" {
        assert_equal noeviction [lindex [r config get maxmemory-policy] 1]
    }
}

start_server {tags {"lfu"}
overrides {maxmemory-policy volatile-lfu}} {
    test "Maxmemory-policy set to allkeys-lfu at startup" {
        assert_equal volatile-lfu [lindex [r config get maxmemory-policy] 1]
    }

    test "Config set maxmemory-policy(volatile-lfu noeviction allkeys_lfu) is allowed" {
        foreach policy {volatile-lfu noeviction allkeys-lfu} {
            assert_equal "OK" [r config set maxmemory-policy $policy]
            assert_equal $policy [lindex [r config get maxmemory-policy] 1]
        }
    }

    test "Config set maxmemory-policy other is forbiden" {
        set prepolicy [lindex [r config get maxmemory-policy] 1]
        foreach policy {volatile-lru volatile-random allkeys-lru null other} {
            puts $policy
            assert_error "ERR*Invalid argument*CONFIG SET*" {r config set maxmemory-policy $policy}
            assert_equal $prepolicy [lindex [r config get maxmemory-policy] 1]
        }
    }
}

# TODO 内存满时优先淘汰冷数据
#start_server {tags {"lfu"}
#overrides {lfu-decay-time 10}} {
#        test "evict cold keys in prior when satisfy evicted condition" {
#            set value [string repeat 'x' 1000]
#            foreach key {key1 key2 key3 key4 key5 key6 key7} {
#                r set $key v
#                r setlfu $key 4
#}
#
#            r config set maxmemory 0
#            after 1000000
#    }
#}
