start_server {tags {"ssdb"}
overrides {save ""}} {
    set ssdb [redis $::host 8888]
    set redis [redis $::host 6379]

    # Enable the AOF
    $redis config set appendonly yes
    $redis config set auto-aof-rewrite-percentage 0 ; # Disable auto-rewrite.

    test "Basic AOF rewrite" {
        $redis set foo bar
        $redis dumptossdb foo

        #wait key dumped to ssdb
        wait_for_condition 100 1 {
            [ $ssdb exists foo ] eq 1
        } else {
            fail "key foo be dumptossdb failed"
        }
        # Load the AOF
        $redis debug loadaof
        list [$redis locatekey foo] [$redis get foo]
        } {ssdb bar}
}
