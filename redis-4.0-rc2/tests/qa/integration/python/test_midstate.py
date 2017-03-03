#!/usr/bin/python
#coding=utf-8
import unittest
import redis
from multiprocessing import Pool, Queue, Manager
import time

class RedisPool:
    def Redis_Pool(self,ClientHost="localhost",ClientPort=6379,ClientDb=0):
        pool=redis.ConnectionPool(host=ClientHost,port=ClientPort,db=ClientDb)
        return redis.StrictRedis(connection_pool=pool)

R = RedisPool().Redis_Pool()

def dumptossdb(key="key"):
    return R.execute_command("dumptossdb "+key)

def locatekey(key="key"):
    return R.execute_command("locatekey "+key)

def strlen(q, key="key"):
    res = "%d:%d:%d" % ( R.strlen(key), q.empty(), R.strlen(key))
    print res

    # 0x1 indicate some read happen during transferring status that blocked write
    if "20000000:1" in res:
        return 0x1

    if "0:20000000" in res:
        print "0:20000000 in res,read not get the updated key after write complete."
        return 0x2

    elif "20000001:1" in res:
        print "20000001:1 in res, read got the updated key before write complete."
        return 0x4

    return 0x8

def append(q, key="key"):
    print "append:%d" % R.append(key, "b")
    q.put('S')

def wait_dump(key="key"):
    attemps = 0
    while attemps < 10:
        time.sleep(0.01)
        if "ssdb" == R.execute_command("locatekey "+key):
            return True
        attemps +=1
    return False

class TestMidState(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print "setUpCLass..."
        attemps = 0
        success = False
        while attemps < 5 and not success:
            success = True
            try:
                R.config_set("maxmemory", "0")
            except Exception, e:
                print 'repr(e): ', repr(e)
                attemps +=1
                success = False
                if attemps == 5:
                    print '10 secs time out and exit!!'
                    os._exit(0)
                time.sleep(2)

    def __init__(self,*args, **kwargs):
        super(TestMidState, self).__init__(*args, **kwargs)

    def setUp(self):
        print "setUp..."
        self.p = Pool(50)
        self.key = "key"
        self.val = 'a'*20000000
        R.set(self.key, self.val)

    def tearDown(self):
        print "tearDown..."
        R.delete(self.key)

    def test_transferring(self):
        '''during transferring key block write and allow read operation'''
        manager = Manager()
        q = manager.Queue()
        flags = 0
        results = []
        dumptossdb()
        self.p.apply_async(append, args = (q,))
        for i in range(100):
            time.sleep(0.001)
            result = self.p.apply_async(strlen, args = (q,))
            results.append(result)

        self.p.close()
        self.p.join()
        for result in results:
            flags |= result.get()
        self.assertEqual(0x9, flags, "no blocked write or read is blocked during transferring status")

    def test_loading_block_write(self):
        '''during loading key block write operation'''
        manager = Manager()
        q = manager.Queue()
        flags = 0
        dumptossdb()
        self.assertTrue(wait_dump(), "wait dump key timeout")
        for i in range(100):
            self.p.apply_async(append, args = (q,))

        self.p.close()
        self.p.join()
        self.assertEqual(20000100, R.strlen(self.key), "no blocked write during loading status")

    @unittest.skip("not availble")
    def test_loading_block_read(self):
        '''during loading key reject write and read operation'''
        manager = Manager()
        q = manager.Queue()
        flags = 0
        results = []
        dumptossdb()
        self.assertTrue(wait_dump(), "wait dump key timeout")
        self.p.apply_async(append, args = (q,))
        for i in range(100):
            time.sleep(0.001)
            self.p.apply_async(append, args = (q,))
            self.p.apply_async(strlen, args = (q,))

        for i in range(0):
            result = self.p.apply_async(strlen, args = (q,))
            results.append(result)

        for result in results:
            flags |= result.get()
        self.p.close()
        self.p.join()
        self.assertEqual(0x9, flags, "no blocked write or blocked read during transferring status")

if __name__=='__main__':
    unittest.main(verbosity=2)
