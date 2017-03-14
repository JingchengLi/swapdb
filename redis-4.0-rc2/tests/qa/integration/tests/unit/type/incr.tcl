start_server {tags {"type"}
overrides {maxmemory 0}} {
    test {INCR against non existing key} {
        set res {}
        append res [ssdbr incr novar]
        append res [ssdbr get novar]
    } {11}

    test {INCR against key created by incr itself} {
        ssdbr incr novar
    } {2}

    test {INCR against key originally set with SET} {
        ssdbr set novar 100
        ssdbr incr novar
    } {101}

    test {INCR over 32bit value} {
        ssdbr set novar 17179869184
        ssdbr incr novar
    } {17179869185}

    test {INCRBY over 32bit value with over 32bit increment} {
        ssdbr set novar 17179869184
        ssdbr incrby novar 17179869184
    } {34359738368}

    test {INCR fails against key with spaces (left)} {
        ssdbr set novar "    11"
        catch {r incr novar} err
        format $err
    } {ERR*}

    test {INCR fails against key with spaces (right)} {
        ssdbr set novar "11    "
        catch {r incr novar} err
        format $err
    } {ERR*}

    test {INCR fails against key with spaces (both)} {
        ssdbr set novar "    11    "
        catch {r incr novar} err
        format $err
    } {ERR*}

    test {INCR fails against a key holding a list} {
        ssdbr rpush mylist 1
        catch {r incr mylist} err
        ssdbr rpop mylist
        format $err
    } {WRONGTYPE*}

    test {DECRBY over 32bit value with over 32bit increment, negative res} {
        ssdbr set novar 17179869184
        ssdbr decrby novar 17179869185
    } {-1}

    test {INCR uses shared objects in the 0-9999 range} {
        ssdbr set foo -1
        ssdbr incr foo
        assert {[ssdbr object refcount foo] > 1}
        ssdbr set foo 9998
        ssdbr incr foo
        assert {[ssdbr object refcount foo] > 1}
        ssdbr incr foo
        assert {[ssdbr object refcount foo] == 1}
    }

    test {INCR can modify objects in-place} {
        ssdbr set foo 20000
        r incr foo
        assert {[r object refcount foo] == 1}
        set old [lindex [split [r debug object foo]] 1]
        r incr foo
        set new [lindex [split [r debug object foo]] 1]
        assert {[string range $old 0 2] eq "at:"}
        assert {[string range $new 0 2] eq "at:"}
        assert {$old eq $new}
    }

    test {INCRBYFLOAT against non existing key} {
        ssdbr del novar
        list    [roundFloat [ssdbr incrbyfloat novar 1]] \
                [roundFloat [ssdbr get novar]] \
                [roundFloat [ssdbr incrbyfloat novar 0.25]] \
                [roundFloat [ssdbr get novar]]
    } {1 1 1.25 1.25}

    test {INCRBYFLOAT against key originally set with SET} {
        ssdbr set novar 1.5
        roundFloat [ssdbr incrbyfloat novar 1.5]
    } {3}

    test {INCRBYFLOAT over 32bit value} {
        ssdbr set novar 17179869184
        ssdbr incrbyfloat novar 1.5
    } {17179869185.5}

    test {INCRBYFLOAT over 32bit value with over 32bit increment} {
        ssdbr set novar 17179869184
        ssdbr incrbyfloat novar 17179869184
    } {34359738368}

    test {INCRBYFLOAT fails against key with spaces (left)} {
        set err {}
        ssdbr set novar "    11"
        catch {r incrbyfloat novar 1.0} err
        format $err
    } {ERR*valid*}

    test {INCRBYFLOAT fails against key with spaces (right)} {
        set err {}
        ssdbr set novar "11    "
        catch {r incrbyfloat novar 1.0} err
        format $err
    } {ERR*valid*}

    test {INCRBYFLOAT fails against key with spaces (both)} {
        set err {}
        ssdbr set novar " 11 "
        catch {r incrbyfloat novar 1.0} err
        format $err
    } {ERR*valid*}

    test {INCRBYFLOAT fails against a key holding a list} {
        ssdbr del mylist
        set err {}
        ssdbr rpush mylist 1
        catch {r incrbyfloat mylist 1.0} err
        ssdbr del mylist
        format $err
    } {WRONGTYPE*}

    test {INCRBYFLOAT does not allow NaN or Infinity} {
        ssdbr set foo 0
        set err {}
        catch {r incrbyfloat foo +inf} err
        set err
        # p.s. no way I can force NaN to test it from the API because
        # there is no way to increment / decrement by infinity nor to
        # perform divisions.
    } {ERR*would produce*}

    test {INCRBYFLOAT decrement} {
        ssdbr set foo 1
        roundFloat [ssdbr incrbyfloat foo -1.1]
    } {-0.1}
}
