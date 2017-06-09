#!/usr/bin/python
#coding=utf-8
import unittest
import redis
from multiprocessing import Pool, Queue, Manager
import time
import sys

class RedisPool:
    def Redis_Pool(self,ClientHost="localhost",ClientPort=sys.argv[1],ClientDb=0):
        pool=redis.ConnectionPool(host=ClientHost,port=ClientPort,db=ClientDb)
        return redis.StrictRedis(connection_pool=pool)

R = RedisPool().Redis_Pool()

'''20M val need about 100ms dump/restore'''
vlen = 20000000

def config_set_maxmemory():
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

def storetossdb(key="key"):
    return R.execute_command("storetossdb "+key)

def dumpfromssdb(key="key"):
    return R.execute_command("dumpfromssdb "+key)

def AllowReadBlockWrite(q, key="key"):
    res = "%d:%d:%d" % ( R.strlen(key), q.empty(), R.strlen(key))

    # some read complete before blocked write complete
    if "%d:1" % vlen in res:
        return 0x1
    # should read the updated key if write complete
    if "0:%d" % vlen in res:
        return 0x2
    # should not read the updated key before write complete
    elif "%d:1" % (vlen+1) in res:
        return 0x4
    # read the updated key after write complete
    elif "0:%d" % (vlen+1) in res:
        return 0x8
    else:
        print "invaild result!"
        return 0x10

def BlockRead(q, key="key"):
    res = "%d:%d" % ( q.empty(),R.strlen(key))
    # read should blocked and read the updated key
    if "%d" % vlen in res:
        return 0x1
    elif "1:%d" % (vlen+1) in res:
        return 0x2
    elif "0:%d" % (vlen+1) in res:
        return 0x4
    else:
        print "invaild result!"
        return 0x10

def append(q, key="key"):
    R.append(key, "b")
    # q.empty() is 0 if append complete.
    q.put('S')

def wait_status(status, key="key"):
    attemps = 0
    while attemps < 50:
        time.sleep(0.01)
        if status == R.execute_command("locatekey "+key):
            return True
        attemps +=1
    return False

class TestMidState(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config_set_maxmemory()

    def __init__(self,*args, **kwargs):
        super(TestMidState, self).__init__(*args, **kwargs)

    def setUp(self):
        self.p = Pool(50)
        self.key = "key"
        self.val = 'a'*vlen
        R.set(self.key, self.val)

    def tearDown(self):
        R.delete(self.key)

    def test_transferring(self):
        '''during transferring key block write and allow read operation'''
        manager = Manager()
        q = manager.Queue()
        flags = 0
        results = []
        storetossdb()
        self.p.apply_async(append, args = (q,))
        time.sleep(0.01)
        for i in range(150):
            time.sleep(0.001)
            result = self.p.apply_async(AllowReadBlockWrite, args = (q,))
            results.append(result)

        self.p.close()
        self.p.join()
        for result in results:
            flags |= result.get()
        self.assertEqual(0x9, flags, "%d : no blocked write or read is blocked during transferring status" % flags)

    def test_loading_block_write(self):
        '''during loading key block write operation'''
        manager = Manager()
        q = manager.Queue()
        flags = 0
        storetossdb()
        self.assertTrue(wait_status("ssdb"), "wait dump key timeout")
        for i in range(1000):
            self.p.apply_async(append, args = (q,))

        self.p.close()
        self.p.join()
        self.assertEqual(20001000, R.strlen(self.key), "%d : mthreads write comflict during loading status" % R.strlen(self.key))

    def test_loading_block_read(self):
        '''during loading key block read operation'''
        manager = Manager()
        q = manager.Queue()
        flags = 0
        results = []
        storetossdb()
        self.assertTrue(wait_status("ssdb"), "wait dump key timeout")
        dumpfromssdb()
        self.p.apply_async(append, args = (q,))
        time.sleep(0.01)
        for i in range(100):
            result = self.p.apply_async(BlockRead, args = (q,))
            results.append(result)

        self.p.close()
        self.p.join()
        for result in results:
            flags |= result.get()
        self.assertEqual(0x6, flags, "%d : read shoule be blocked during loading status" % flags)

# not support setup etc in below python 2.7.
config_set_maxmemory()
suite = unittest.TestLoader().loadTestsFromTestCase(TestMidState)
unittest.TextTestRunner(verbosity=2).run(suite)
#  if __name__=='__main__':
    #  unittest.main(verbosity=2)
