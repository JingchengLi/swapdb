start_server {tags {"ssdb"}} {
    r flushall
    test "Repopid basics" {
        assert_equal "repopid 0 0" {*}[r repopid get] ;#repopid 0 0
        r repopid set 1 2
        r set foo bar12
        assert_equal "repopid 1 2" {*}[r repopid get] ;#repopid 1 2
    }

    test "Repopid after flushall" {
        r flushall
        assert_equal "repopid 0 0" {*}[r repopid get] ;#repopid 0 0
        r repopid set 2 3
        r set foo bar23
        assert_equal "repopid 2 3" {*}[r repopid get] ;#repopid 2 3
    }

    test "Repopid not update if no real ops" {
        r repopid set 3 4
        assert_equal "repopid 2 3" {*}[r repopid get] ;#repopid 2 3
    }

    test "Repopid update with real ops" {
        r set foo bar34
        assert_equal "repopid 3 4" {*}[r repopid get] ;#repopid 3 4
    }

    test "Repopid abnormal update" {
        r repopid set 2 5 ;#receive timestamp earlier
        r set foo bar25
        puts [r repopid get] ;#????

        r repopid set 4 3 ;#receive id smaller
        r set foo bar43
        puts [r repopid get] ;#????

        r repopid set 5 6 ;#receive id skip from 4->6
        r set foo bar56
        puts [r repopid get] ;#????
    }

    test "Repopid slave->master(reset repopid) by two write ops" {
        r set foo bar
        assert_equal "repopid 0 0" {*}[r repopid get] ;#repopid 0 0
    }

    test "Repopid master->slave" {
        r repopid set 1 2
        r set foo bar12
        assert_equal "repopid 1 2" {*}[r repopid get] ;#repopid 1 2
    }

    # TODO get after set cannot reset repopid
    test "Repopid slave->master(reset repopid) by write/read ops" {
        r get foo
        assert_equal "repopid 0 0" {*}[r repopid get] ;#repopid 0 0
    }

    test "Repopid no update with read ops" {
        r repopid set 1 2
        r get foo
        assert_equal "repopid 0 0" {*}[r repopid get] ;#repopid 0 0
    }

    # TODO get will clear repopid set, so set after it will set no repopid
    test "Repopid reset with write ops after get" {
        r set foo bar12
        assert_equal "repopid 0 0" {*}[r repopid get] ;#repopid 0 0
    }

    test "Repopid process receivedID < 0" {
        r repopid set 2 3
        r set foo bar23
        r repopid set 3 -1
        r set foo bar3_1
        assert_equal "repopid 2 3" {*}[r repopid get] ;#repopid 2 3
    }

    test "Repopid can process args which is not available" {
        catch {r repopid ?} err
        assert_match "ERR*" $err
        catch {r repopid get 1} err
        puts "r repopid get 1:$err"
        catch {r repopid get a} err
        puts "r repopid get a:$err"
        catch {r repopid set 1} err
        puts "r repopid set 1:$err"
        catch {r repopid set 1 2 3} err
        puts "r repopid set 1 2 3:$err"
        catch {r repopid set a 2} err
        puts "r repopid set a 2:$err"
        catch {r repopid set 1 a} err
        puts "r repopid set 1 a:$err"

        puts "repopid get:[r repopid get]"
        r set foo bar
        puts "repopid get:[r repopid get]"
        catch {r repopid set 5 6} err
        puts "r repopid set 5 6:$err"
        puts "repopid get:[r repopid get]"
        r set foo bar56
        puts "repopid get:[r repopid get]"
    }
}
