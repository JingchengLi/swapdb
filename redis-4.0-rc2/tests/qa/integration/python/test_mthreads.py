#!/usr/bin/python
#coding=utf-8
import unittest
import redis
from multiprocessing import Pool
import os, time, random, sys

class RedisPool:
    def Redis_Pool(self,ClientHost="localhost",ClientPort=sys.argv[1],ClientDb=0):
        pool=redis.ConnectionPool(host=ClientHost,port=ClientPort,db=ClientDb)
        return redis.StrictRedis(connection_pool=pool)

R = RedisPool().Redis_Pool()

def set(key="key"):
    val = 'a'*random.randint(100, 20000)
    return R.set(key, val)

def get(key="key"):
    return R.get(key)

def delete(key="key"):
    return R.delete(key)

def storetossdb(key="key"):
    return R.execute_command("storetossdb "+key)

def locatekey(key="key"):
    return R.execute_command("locatekey "+key)

class TestMthreads(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print "setUpCLass..."
        attemps = 0
        success = False
        while attemps < 5 and not success:
            success = True
            try:
                R.config_set("maxmemory", "2000mb")
            except Exception, e:
                print 'repr(e): ', repr(e)
                attemps +=1
                success = False
                if attemps == 5:
                    print '10 secs time out and exit!!'
                    os._exit(0)
                time.sleep(2)

    def __init__(self,*args, **kwargs):
        super(TestMthreads, self).__init__(*args, **kwargs)

    def setUp(self):
        print "setUp..."
        self.p = Pool(50)
        self.key = "key"

    def tearDown(self):
        print "tearDown..."

    def waitMemoryStable(self):
        n, memory = 0, 0
        while memory != R.info("memory")["used_memory"] and n < 100:
            memory = R.info("memory")["used_memory"]
            time.sleep(0.2)
            n+=1
        self.assertNotEqual(100, n, "more than 20 secs to wait memory stable.")
        return memory

    #  @unittest.skip("skip test_01")
    def test_01(self):
        '''set/storetossdb/get concurrency'''
        for i in range(100):
            self.p.apply_async(set)
            self.p.apply_async(storetossdb)
            self.p.apply_async(get)

        self.p.close()
        self.p.join()
        print "At last key(%s)'s status is %s!" % (self.key, locatekey())
        self.assertTrue(locatekey() in ["redis", "ssdb"])
        #  self.assertIn(locatekey(),["redis", "ssdb"])
        R.delete(self.key)

    #  @unittest.skip("skip test_01_leak")
    def test_01_leak(self):
        '''set/storetossdb/get concurrency memory leak'''
        memory_before = R.info("memory")["used_memory"]
        for i in range(100):
            self.p.apply_async(set)
            self.p.apply_async(storetossdb)
            self.p.apply_async(get)

        self.p.close()
        self.p.join()
        memory_after = self.waitMemoryStable()

        self.assertTrue(locatekey() in ["redis", "ssdb"])
        try:
            self.assertTrue(memory_after-R.execute_command("memory usage "+self.key)-50000 <= memory_before)
            self.assertEqual(locatekey(),"redis")
        except TypeError:
            self.assertTrue(memory_after-50000 <= memory_before)
            self.assertEqual(locatekey(),"ssdb")
        R.delete(self.key)

    #  TODO need to check after lower 0.9
    #  @unittest.skip("skip test_02")
    def test_02(self):
        '''All keys still exist after reach 0.9*maxmemory'''
        keysNum = 2000

        for i in range(keysNum):
            self.p.apply_async(set, args=(self.key+str(i),))
            #  print "clients:%d" % R.info("Clients")["connected_clients"]

        self.p.close()
        self.p.join()
        self.waitMemoryStable()
        ssdbnum = 0
        for i in range(keysNum):
            self.assertNotEqual(locatekey(self.key+str(i)),"none",self.key+str(i))
            if locatekey(self.key+str(i)) == "ssdb":
                    ssdbnum+=1

        print "ssdbnum is %d" % ssdbnum
        print "totalnum is %d" % R.dbsize()

        for i in range(keysNum):
            self.assertTrue(R.delete(self.key+str(i)))

    #  @unittest.skip("skip test_03")
    def test_03(self):
        '''repeat set/get to load from ssdb after dump'''
        keysNum = 2000
        for i in range(10):
            for j in range(keysNum):
                self.p.apply_async(set, args=(self.key+str(j),))
                self.p.apply_async(get, args=(self.key+str(j),))

        self.p.close()
        self.p.join()
        self.waitMemoryStable()
        ssdbnum = 0
        for i in range(keysNum):
            self.assertNotEqual(locatekey(self.key+str(i)),"none",self.key+str(i))
            if locatekey(self.key+str(i)) == "ssdb":
                    ssdbnum+=1

        print "ssdbnum is %d" % ssdbnum
        print "totalnum is %d" % R.dbsize()

        for i in range(keysNum):
            self.assertTrue(R.delete(self.key+str(i)))

    #  @unittest.skip("skip test_04")
    def test_04(self):
        '''del/set to load from ssdb after dump'''
        print "start test_04"
        keysNum = 2000
        for j in range(keysNum):
            self.p.apply_async(set, args=(self.key+str(j),))

        self.p.close()
        self.p.join()
        self.waitMemoryStable()

        self.p = Pool(50)
        for i in range(keysNum):
            self.p.apply_async(delete, args=(self.key+str(i),))
            self.p.apply_async(set, args=(self.key+str(i),))
            print "process key %d" % i

        self.p.close()
        self.p.join()
        self.waitMemoryStable()

        ssdbnum, nonenum = 0, 0
        for i in range(keysNum):
            #  self.assertNotEqual(locatekey(self.key+str(i)),"none",self.key+str(i))
            if locatekey(self.key+str(i)) == "ssdb":
                ssdbnum+=1
            if locatekey(self.key+str(i)) == "none":
                nonenum+=1
                print "check key none %d" % i

        print "ssdbnum is %d" % ssdbnum
        print "totalnum is %d" % R.dbsize()
        print "nonenum is %d" % nonenum

suite = unittest.TestLoader().loadTestsFromTestCase(TestMthreads)
unittest.TextTestRunner(verbosity=2).run(suite)
#if __name__=='__main__':
#    print 'Parent process %s.' % os.getpid()
#    unittest.main(verbosity=2)
#    print 'All subprocesses done.'
