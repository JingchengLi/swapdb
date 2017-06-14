start_server {tags {"introspection"}} {
    test {TTL and TYPYE do not alter the lfu count of a key} {
        ssdbr set foo bar
        after 3000
        ssdbr ttl foo
        ssdbr type foo
        assert {[ssdbr object freq foo] == 5}
    }
}
