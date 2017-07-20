#!/usr/bin/python
#coding=utf-8
import unittest
import redis
import time
import sys
from random import randint

class RedisPool:
    def Redis_Pool(self,ClientHost="localhost",ClientPort=sys.argv[1],ClientDb=0):
        pool=redis.ConnectionPool(host=ClientHost,port=ClientPort,db=ClientDb)
        return redis.StrictRedis(connection_pool=pool)

class TestPipeline(unittest.TestCase):
    def __init__(self,*args, **kwargs):
        print "initClass..."
        super(TestPipeline, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        print "setUpCLass..."

    def setUp(self):
        print "setUp..."
        self.R = RedisPool().Redis_Pool()
        self.R.flushall()
        self.R.set("nullkey", "nullvalue")
        while True:
            try:
                self.R.execute_command("storetossdb "+"nullkey")
            except redis.exceptions.ResponseError:
                time.sleep(0.1)
            else:
                self.R.delete("nullkey")
                break

    def tearDown(self):
        print "tearDown..."

    def dumpKeys(self, patten='*'):
        for key in self.R.keys():
            self.R.execute_command("storetossdb "+key)
        time.sleep(0.5)

    def test_pipleline_set_get(self, r=RedisPool().Redis_Pool()):
        self.R.set("foo", 'ssdbbar')
        self.dumpKeys()

        with r.pipeline(False) as pipe:
            expect = [True, 'bar']
            pipe.set("foo", "bar");
            pipe.get("foo");
            result = pipe.execute()
            self.assertEqual(expect, result)

    def test_pipelineResponse(self, r=RedisPool().Redis_Pool()):
        self.R.set("stringkey", "foo")
        self.R.lpush("listkey", "foo")
        self.R.hset("hashkey", "foo", "bar")
        self.R.zadd("zsetkey", 1, "foo")
        self.R.sadd("setkey", "foo")
        self.R.setrange("setrangekey", 0, "0123456789");
        self.dumpKeys()

        #  bytesForSetRange = [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 ]
        #  self.R.setrange("setrangebytes".getBytes(), 0, bytesForSetRange);
        with r.pipeline(False) as pipe:
            pipe.get("stringkey")
            pipe.lpop("listkey")
            pipe.hget("hashkey", "foo")
            pipe.zrange("zsetkey", 0, -1)
            pipe.spop("setkey")

            pipe.exists("listkey")
            pipe.zincrby("zsetkey", "foo", 1)
            pipe.zcard("zsetkey")
            pipe.lpush("listkey", "bar")
            pipe.lrange("listkey", 0, -1)
            pipe.hgetall("hashkey")
            pipe.sadd("setkey", "foo")
            pipe.smembers("setkey")
            pipe.zrange("zsetkey", 0, -1, withscores=True)
            pipe.getrange("setrangekey", 1, 3)
            #  getrangeBytes = pipeline.getrange("setrangebytes".getBytes(), 6, 8);

            expect = ['foo', 'foo', 'bar', ['foo'], 'foo', False, 2.0, 1, 1L, ['bar'], {'foo': 'bar'}, 1, set(['foo']), [('foo', 2.0)], '123']
            result = pipe.execute()
            self.assertEqual(expect, result)

    def test_pipelineResponseWithData(self, r=RedisPool().Redis_Pool()):
        self.R.zadd("zset", 1, "foo")
        self.dumpKeys()
        with r.pipeline(False) as pipe:
            pipe.zscore("zset", "foo")
            expect = [1.0]
            result = pipe.execute()
            self.assertEqual(expect, result)

    def test_basepipeline(self, r=RedisPool().Redis_Pool()):
        with r.pipeline(False) as pipe:
            pipe.set('a', 'a1').get('a').zadd('z', z1=1).zadd('z', z2=4)
            pipe.zincrby('z', 'z1').zrange('z', 0, 5, withscores=True)
            assert pipe.execute() == \
                [
                    True,
                    'a1',
                    True,
                    True,
                    2.0,
                    [('z1', 2.0), ('z2', 4)],
                ]
            self.dumpKeys()
            pipe.set('a', 'a1').get('a').zadd('z', z1=1).zadd('z', z2=4)
            pipe.zincrby('z', 'z1').zrange('z', 0, 5, withscores=True)
            expect = \
                [
                    True,
                    'a1',
                    False,
                    False,
                    2.0,
                    [('z1', 2.0), ('z2', 4)],
                ]
            result = pipe.execute()
            self.assertEqual(expect, result)

    def test_pipeline_length(self, r=RedisPool().Redis_Pool()):
        with r.pipeline(False) as pipe:
            # Initially empty.
            assert len(pipe) == 0
            assert not pipe

            pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
            assert len(pipe) == 3
            assert pipe

            # Execute calls reset(), so empty once again.
            pipe.execute()
            assert len(pipe) == 0
            assert not pipe
            self.dumpKeys()
            pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
            assert len(pipe) == 3
            assert pipe

            pipe.execute()
            assert len(pipe) == 0
            assert not pipe

    def test_exec_error_in_response(self, r=RedisPool().Redis_Pool()):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        with r.pipeline(False) as pipe:
            pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4).execute()
            self.dumpKeys()

            result = pipe.get('a').get('b').get('c').get('d').execute(raise_on_error=False)

            assert isinstance(result[2], redis.ResponseError)
            self.assertEqual('1', result[0])
            self.assertEqual('2', result[1])
            self.assertEqual('4', result[3])

    def test_exec_error_raised(self, r=RedisPool().Redis_Pool()):
        with r.pipeline(False) as pipe:
            pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4).execute()
            self.dumpKeys()
            pipe.get('a').get('b').get('c').get('d')
            #  with self.assertRaises(redis.ResponseError) as ex:
                #  print pipe.execute()

            self.assertRaises(redis.ResponseError, pipe.execute)
            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]

    def test_long_pipe(self, r=RedisPool().Redis_Pool()):
        base=2000
        with r.pipeline(False) as pipe:
            for n in range(10*base):
                pipe.set("key"+str(n), "foo"+str(n))
                if(0 == n%10):
                    pipe.delete("key"+str(n))
                if(randint(1,10) < 5):
                    pipe.execute_command("storetossdb "+"key"+str(n))
            result = pipe.execute()

        with r.pipeline(False) as pipe:
            for n in range(10*base):
                if(0 == n%8):
                    pipe.set("key"+str(n), str(n)+"foo")
                pipe.get("key"+str(n))
            result = pipe.execute()

        m=0
        for n in range(10*base):
            if(0 == n%8):
                self.assertEqual(True, result[n+m])
                m+=1
                self.assertEqual(str(n)+"foo", result[n+m])
            elif(0 == n%10):
                self.assertEqual(None, result[n+m])
            else:
                self.assertEqual("foo"+str(n), result[n+m])

suite = unittest.TestLoader().loadTestsFromTestCase(TestPipeline)
unittest.TextTestRunner(verbosity=2).run(suite)
#  if __name__=='__main__':
    #  unittest.main(verbosity=2)
