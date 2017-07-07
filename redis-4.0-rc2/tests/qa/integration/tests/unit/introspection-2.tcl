start_server {tags {"introspection"}
overrides {lfu-decay-time 1}} {
    test {TTL and TYPE do not alter the lfu count of a key} {
        ssdbr set foo bar
        ssdbr ttl foo
        ssdbr type foo
        assert {[ssdbr object freq foo] == 5}
    }
}
