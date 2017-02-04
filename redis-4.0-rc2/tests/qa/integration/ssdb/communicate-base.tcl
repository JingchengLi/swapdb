# exec killall -9 redis-server
# should start a ssdb-server first.
start_server {tags {"ssdb"}} {
    #TODO ssdb should start when redis start
    set ssdbcfgfile ./ssdb/redis_with_ssdb.conf
    set nossdbcfgfile ./ssdb/redis_without_ssdb.conf
    set ssdbpid [exec ../../../src/redis-server $ssdbcfgfile > /dev/null &]
    set nossdbpid 0
    set redis_no_ssdb {}

    after 1000
    set ssdb [redis 127.0.0.1 8888]
    set redis [redis 127.0.0.1 6379]

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

    test "Hot key only store in redis" {
        $redis del foo
        
        $redis set foo bar
        list [$redis locatekey foo] [ $ssdb get foo ] [ $redis get foo ] 
    } {redis {} bar}

    test "Key(become cold) move from redis to ssdb" {
        $redis dumptossdb foo
        $redis set fooxxx barxxx

        #wait key dumped to ssdb
        wait_for_condition 100 10 {
            [ $ssdb get foo ] eq {bar}
        } else {
            fail "key foo be dumptossdb failed"
        }

        $redis bgsave
        #start a test redis-server not connect to ssdb.
        set nossdbpid [exec ../../../src/redis-server $nossdbcfgfile > /dev/null &]
        after 1000
        set redis_no_ssdb [redis 127.0.0.1 6389]
        assert_equal "barxxx" [$redis_no_ssdb get fooxxx] "fooxxx stored in redis"
        list [$redis locatekey foo] [ $redis_no_ssdb get foo ] [ $ssdb get foo ]
    } {ssdb {} bar}

    test "Redis can read key stored in ssdb" {
        $redis get foo
    } {bar}

    test "GET Key(become hot) move from ssdb to redis" {
        list [$redis locatekey foo] [ $ssdb get foo ]
    } {redis {}}

    test "Key(become cold) move from redis to ssdb" {
        $redis dumptossdb foo

        #wait key dumped to ssdb
        wait_for_condition 100 10 {
            [ $ssdb get foo ] eq {bar}
        } else {
            fail "key foo be dumptossdb failed"
        }

        $redis locatekey foo 
    } {ssdb}

    test "SET Key(become hot) move from ssdb to redis" {
        $redis set foo bar1
        list [$redis locatekey foo] [$ssdb get foo] [$redis get foo] 
    } {redis {} bar1}

    test "Redis can DEL key loaded from ssdb to redis" {
        $redis del foo
        list [$redis locatekey foo] [$ssdb get foo] [$redis get foo]
    } {none {} {}}

    test "Key(become cold) move from redis to ssdb" {
        # TODO
        $redis set foo bar
        $redis dumptossdb foo

        #wait key dumped to ssdb
        wait_for_condition 100 10 {
            [ $ssdb get foo ] eq {bar}
        } else {
            fail "key foo be dumptossdb failed"
        }

        $redis locatekey foo
    } {ssdb}

    test "Redis can DEL key stored in ssdb" {
        $redis del foo
        list [$redis locatekey foo] [$ssdb get foo] [$redis get foo]
    } {none {} {}}

    test "SET new key(Hot key) not store in ssdb" {
        $redis set foo bar
        list [$ssdb get foo] [$redis get foo]
    } {{} bar}

    test "GET key(Hot key) store in redis not operate ssdb" {
        set precalls [ get_total_calls "total_calls" $ssdb]
        $redis get foo
        set nowcalls [ get_total_calls "total_calls" $ssdb]
        expr $nowcalls-$precalls
    } 1

    test "GET key(not exist) not operate ssdb" {
        set precalls [ get_total_calls "total_calls" $ssdb]
        $redis get fooxxx
        set nowcalls [ get_total_calls "total_calls" $ssdb]
        expr $nowcalls-$precalls
    } 1

    catch {exec kill -9 $nossdbpid}
    catch {exec kill -9 $ssdbpid}
}
