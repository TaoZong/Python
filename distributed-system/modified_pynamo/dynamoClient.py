'''
Created on Apr 14, 2013

@author: yaolizheng
'''
import time
import random
import zerorpc
import pickle
import gevent
from dynamomessages import ClientPut, ClientGet
from dynamomessages import ClientGetRsp, ClientPutRsp

class DynamoClient():

    def __init__(self, addr):
        self.m_addr = "127.0.0.1:29009"
        self.last_msg = None
        self.addr = addr
        self.nodelist = []
        self.servers = []
        self.count = 0
        self.count2 = 0
        c = zerorpc.Client(timeout=3)
        c.connect('tcp://' + self.m_addr)
        try:
            self.servers = list(c.getNodes())
            print self.servers
            for server in self.servers:
                t = zerorpc.Client(timeout=3)
                t.connect('tcp://' + server)
                self.nodelist.append(t)
            c.close()
        except:
            pass
    
        
    def put(self, key, metadata, value, destnode=None):
        while len(self.nodelist) > 0:
            if destnode is None:  # Pick a random node to send the request to
                destnode = random.choice(self.nodelist)
            try:
                destnode.put_message(self.addr, key, value, pickle.dumps(metadata))
                break
            except:
                self.nodelist.remove(destnode)
                destnode = None

    def get(self, key, destnode=None):
        while len(self.nodelist) > 0:
            if destnode is None:  # Pick a random node to send the request to
                destnode = random.choice(self.nodelist)
            try:
                destnode.get_message(self.addr, key,)
                break
            except:
                self.nodelist.remove(destnode)
                destnode = None

    def rcvmsg(self, msg):
        msg = pickle.loads(msg)
        self.last_msg = msg
        if isinstance(msg, ClientPutRsp):
            #print msg.value
            self.count2 = self.count2 + 1
        if isinstance(msg, ClientGetRsp):
            #print msg.value
            self.count = self.count + 1

    def mr(self, num):    
        c = zerorpc.Client(timeout=3)
        c.connect('tcp://' + self.m_addr)
        try:
            c.map_reduce(num)
        except:
            pass
        c.close()
        

if __name__ == '__main__':
    client='127.0.0.1:29008'
    DClient = DynamoClient(client)
    s = zerorpc.Server(DClient)
    s.bind('tcp://' + client)
    gevent.spawn(s.run)
    print "start"
    starttime = int(round(time.time() * 1000))
    for i in range(0, 1000):
        DClient.put('K' + str(i), None, "I am pig" + str(i))
    endtime = int(round(time.time() * 1000))
    print "put time: " + str(endtime - starttime)
    starttime = int(round(time.time() * 1000))
    for i in range(0, 1000):
        DClient.get('K' + str(i))
    endtime = int(round(time.time() * 1000))
    print "get time: " + str(endtime - starttime)
        
   # print DClient.count2
   # print DClient.count
    
    DClient.mr(2)