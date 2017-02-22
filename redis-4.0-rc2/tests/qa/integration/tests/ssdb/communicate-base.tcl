start_server {tags {"ssdb"}} {
    set ssdb [redis $::host $::ssdbport]

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
            r flushall
            r del foo
            $ssdb flushdb

            if {$ttl > 0} {
                r setex foo $ttl bar
            } else {
                r set foo bar
            }

            wait_for_restoreto_redis r foo

            assert {[$ssdb get foo] eq {}}
            r get foo
        } {bar}

        test "Key(become cold) with ttl($ttl) check with jdjr-mode" {
            r dumptossdb foo
            r set fooxxx barxxx

            wait_for_dumpto_ssdb r foo
            r config set jdjr-mode no

            list [r get fooxxx] [ r get foo ]
        } {barxxx {}}

        test "Redis can read key stored in ssdb with ttl($ttl)" {
            r config set jdjr-mode yes
            r get foo
        } {bar}

        test "GET Key(become hot) - 1 move from ssdb to redis with ttl($ttl)" {
            wait_for_restoreto_redis r foo
            $ssdb get foo
        } {}

        test "Key(become cold) - 2 move from redis to ssdb with ttl($ttl)" {
            r dumptossdb foo

            wait_for_dumpto_ssdb r foo
            $ssdb get foo
        } {bar}

        test "SET Key(become hot) - 3 move from ssdb to redis with ttl($ttl)" {
            if {$ttl > 0} {
                r setex foo $ttl bar1
            } else {
                r set foo bar1
            }
            wait_for_restoreto_redis r foo

            list [$ssdb get foo] [r get foo]
        } {{} bar1}

        test "Redis can DEL key loaded from ssdb to redis with ttl($ttl)" {
            r del foo
            list [r locatekey foo] [$ssdb get foo] [r get foo]
        } {none {} {}}

        test "Key(become cold) - 4 move from redis to ssdb with ttl($ttl)" {
            # TODO
            if {$ttl > 0} {
                r setex foo $ttl bar
            } else {
                r set foo bar
            }
            r dumptossdb foo

            wait_for_dumpto_ssdb r foo

            $ssdb get foo
        } {bar}

        test "Redis can DEL key stored in ssdb with ttl($ttl)" {
            r del foo
            list [r locatekey foo] [$ssdb get foo] [r get foo]
        } {none {} {}}

        test "SET new key(Hot key) not store in ssdb with ttl($ttl)" {
            if {$ttl > 0} {
                r setex foo $ttl bar
            } else {
                r set foo bar
            }
            wait_for_restoreto_redis r foo

            list [$ssdb get foo] [r get foo]
        } {{} bar}

        test "GET key(Hot key) store in redis not operate ssdb with ttl($ttl)" {
            set precalls [ get_total_calls "total_calls" $ssdb]
            r get foo
            set nowcalls [ get_total_calls "total_calls" $ssdb]
            expr $nowcalls-$precalls
        } 1

        test "GET key(not exist) not operate ssdb with ttl($ttl)" {
            set precalls [ get_total_calls "total_calls" $ssdb]
            r del fooxxx
            r get fooxxx
            set nowcalls [ get_total_calls "total_calls" $ssdb]
            expr $nowcalls-$precalls
        } 1

        if {$ttl > 0} {
            test "key store in ssdb with ttl(3) will expire" {
                r setex foo 3 bar
                r dumptossdb foo

                wait_for_dumpto_ssdb r foo

                after 3100
                list [r locatekey foo] [r get foo]
            } {none {}}
        }
    }
}
