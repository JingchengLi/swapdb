start_server {tags {"ssdb"}
overrides {maxmemory 0}} {
    test "#crash issue Reply from SSDB:ERR value*integer*out of range" {
        r set foo bar
        dumpto_ssdb_and_wait r foo
        assert_error "*out of range*" {r incr foo}
        assert_error "*out of range*" {r incr foo}
    }

    test "new key's lfu is 5" {
        r set foo bar
        assert_equal 5 [r object freq foo] "[r object freq foo] not equal 5"
        r del foo
    }

    test "object freq can count key's lfu in ssdb" {
        r set foo bar
        dumpto_ssdb_and_wait r foo
        assert_equal 5 [r object freq foo] ""
        r del foo
    }

    test "#issue lfu count be initialled to 5 when key dump to ssdb " {
        for {set i 0} {$i < 1000} {incr i} {
            r incr foo
        }
        set freq [r object freq foo]
        assert {$freq > 5}
        dumpto_ssdb_and_wait r foo
        assert_equal $freq [r object freq foo] "ssdb freq:[r object freq foo] wrong"
        r del foo
    }
}
