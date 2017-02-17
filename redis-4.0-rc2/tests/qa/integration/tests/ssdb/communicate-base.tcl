start_server {tags {"ssdb"}} {
    set ssdb [redis $::host 8888]
    set redis [redis $::host 6379]

    proc get_total_calls { s ssdb } {
        set info [$ssdb info]
        set len [string length $s]
        set start [string first $s $info]
        set end [string first " " $info [expr $start+$len+1 ]]
        string range $info [expr $start+$len+1 ] $end-1
    }

    test "Ssdb is up" {
        $ssdb ping
    } {PONG}

    test "ToDo Ssdb connect to redis" {
        #TODO:currently redis cannot startup if no connect to ssdb
        #redis should have some flag/status to check this
    }

    foreach ttl {0 1000} {
        test "Initialize Hot key only store in redis with ttl($ttl)" {
            $redis flushall
            $redis del foo
            $ssdb flushdb

            if {$ttl > 0} {
                $redis setex foo $ttl bar
            } else {
                $redis set foo bar
            }

            wait_for_restoreto_redis $redis foo

            assert {[$ssdb get foo] eq {}}
            $redis get foo
        } {bar}

        test "Key(become cold) with ttl($ttl) check with jdjr-mode" {
            $redis dumptossdb foo
            $redis set fooxxx barxxx

            wait_for_dumpto_ssdb $redis foo
            $redis config set jdjr-mode no

            list [$redis get fooxxx] [ $redis get foo ]
        } {barxxx {}}

        test "Redis can read key stored in ssdb with ttl($ttl)" {
            $redis config set jdjr-mode yes
            $redis get foo
        } {bar}

        test "GET Key(become hot) - 1 move from ssdb to redis with ttl($ttl)" {
            wait_for_restoreto_redis $redis foo
            $ssdb get foo
        } {}

        test "Key(become cold) - 2 move from redis to ssdb with ttl($ttl)" {
            $redis dumptossdb foo

            wait_for_dumpto_ssdb $redis foo
            $ssdb get foo
        } {bar}

        test "SET Key(become hot) - 3 move from ssdb to redis with ttl($ttl)" {
            if {$ttl > 0} {
                $redis setex foo $ttl bar1
            } else {
                $redis set foo bar1
            }
            wait_for_restoreto_redis $redis foo

            list [$ssdb get foo] [$redis get foo]
        } {{} bar1}

        test "Redis can DEL key loaded from ssdb to redis with ttl($ttl)" {
            $redis del foo
            list [$redis locatekey foo] [$ssdb get foo] [$redis get foo]
        } {none {} {}}

        test "Key(become cold) - 4 move from redis to ssdb with ttl($ttl)" {
            # TODO
            if {$ttl > 0} {
                $redis setex foo $ttl bar
            } else {
                $redis set foo bar
            }
            $redis dumptossdb foo

            wait_for_dumpto_ssdb $redis foo

            $ssdb get foo
        } {bar}

        test "Redis can DEL key stored in ssdb with ttl($ttl)" {
            $redis del foo
            list [$redis locatekey foo] [$ssdb get foo] [$redis get foo]
        } {none {} {}}

        test "SET new key(Hot key) not store in ssdb with ttl($ttl)" {
            if {$ttl > 0} {
                $redis setex foo $ttl bar
            } else {
                $redis set foo bar
            }
            wait_for_restoreto_redis $redis foo

            list [$ssdb get foo] [$redis get foo]
        } {{} bar}

        test "GET key(Hot key) store in redis not operate ssdb with ttl($ttl)" {
            set precalls [ get_total_calls "total_calls" $ssdb]
            $redis get foo
            set nowcalls [ get_total_calls "total_calls" $ssdb]
            expr $nowcalls-$precalls
        } 1

        test "GET key(not exist) not operate ssdb with ttl($ttl)" {
            set precalls [ get_total_calls "total_calls" $ssdb]
            $redis del fooxxx
            $redis get fooxxx
            set nowcalls [ get_total_calls "total_calls" $ssdb]
            expr $nowcalls-$precalls
        } 1

        if {$ttl > 0} {
            test "key store in ssdb with ttl(3) will expire" {
                $redis setex foo 3 bar
                $redis dumptossdb foo

                wait_for_dumpto_ssdb $redis foo

                after 3100
                list [$redis locatekey foo] [$redis get foo]
            } {none {}}
        }
    }
}
