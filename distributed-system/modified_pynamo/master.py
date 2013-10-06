'''
Created on May 1, 2013

@author: ZT
'''
import time
import zerorpc
import gevent
import threading
import random
from gevent import monkey
monkey.patch_all()
from hash_multiple import ConsistentHashTable

class Master():
    def __init__(self, addr):
        self.nodelist = []
        self.map_nodelist = []
        self.map_workers = {}
        self.reduce_nodelist= []
        self.reduce_workers = {}
        self.addr = addr
        self.map_d = {}
        self.reduce_d = {}
        self.reduce_d2 = {}
        self.flag = False
        self.connections = []
        self.starttime = 0
        self.endtime = 0
        
    def add_node(self, remoteaddr):
        print 'add'
        print remoteaddr
        if remoteaddr not in self.nodelist:
            self.nodelist.append(remoteaddr)
#            c = zerorpc.Client(timeout=3)
#            c.connect('tcp://' + remoteaddr)
#            self.connections.append(c)
        print self.nodelist
#        for server in self.connections:
#            try:
#                server.getNodeList(self.nodelist)
#            except:
#                pass
        self.flag = True
            
    def check(self):
        while True:
            if self.flag:
                for server in self.nodelist:
                    c = zerorpc.Client(timeout = 3)
                    c.connect('tcp://' + server)
                    try:
                        c.getNodeList(self.nodelist)
                    except:
                        pass
                    c.close()
                self.flag = False
            gevent.sleep(1)
            
    def getNodes(self):
        return self.nodelist
    
    def get_nodelist(self):
        return self.nodelist
    
    def start(self):
        self.pool = gevent.pool.Group()
        self.pool.spawn(self.check)
        self.pool.spawn(self.detect_map_failure)
        self.pool.spawn(self.detect_reduce_failure)
        
    def map_reduce(self, num):
        self.starttime = int(round(time.time() * 1000))
        self.mapNum = num
        self.reduceComplete = 0
        self.map_nodelist = []
        tmplist = []
        for item in self.nodelist:
            tmplist.append(item)
        for i in range(num):
            node = random.choice(tmplist)
            print node + "====================="
            gevent.sleep(1)
            self.map_nodelist.append(node)
            tmplist.remove(node)
        self.reduce_nodelist = tmplist
        for map_node in self.map_nodelist:
            self.map_workers[map_node] = "Idle"
        for reduce_node in self.reduce_nodelist:
            self.reduce_workers[reduce_node] = "Idle"
            self.reduce_d[reduce_node] = reduce_node
        self.map()
        
    def detect_map_failure(self):
        while True:
            gevent.sleep(1)
            for map_node in self.map_workers:
                if self.map_workers[map_node] == "Working":
                    c = zerorpc.Client(timeout = 3)
                    c.connect("tcp://" + map_node)
                    try:
                        c.are_you_there()
                        c.close()
                    except zerorpc.TimeoutExpired:
                        print map_node + " is down when mapping."
                        if map_node in self.map_nodelist:
                            self.map_nodelist.remove(map_node)
                        if map_node in self.map_workers.keys():
                            self.map_workers[map_node] = "down"
                        flag = True
                        while flag:
                            gevent.sleep(1)
                            for node in self.map_nodelist:
                                print node
                                if self.map_workers[node] == "Idle":
                                    print node + " will do " + map_node + "'s work."
                                    self.map_d[node] = self.map_d[map_node]
                                    self.map_d.pop(map_node)
                                    th = threading.Thread(target=self.assign_map,args=(node, self.map_d))
                                    th.start()
                                    flag = False
                                    break
                    c.close()       
        
    def detect_reduce_failure(self):
        while True:
            gevent.sleep(1)
            for reduce_node in self.reduce_workers:
                if self.reduce_workers[reduce_node] == "Working":
                    c = zerorpc.Client(timeout = 3)
                    c.connect("tcp://" + reduce_node)
                    try:
                        c.are_you_there()
                        c.close()
                    except zerorpc.TimeoutExpired:
                        print reduce_node + " is down when reducing."
                        flag = True
                        while flag:
                            gevent.sleep(1)
                            for node in self.reduce_nodelist:
                                if self.reduce_workers[node] == "Idle":
                                    print node + " will do " + reduce_node + "'s work."
                                    self.reduce_d[reduce_node] = node
                                    print self.reduce_d
                                    for node2 in self.map_workers.keys():
                                        if self.map_workers[node2] == "Idle":
                                            th = threading.Thread(target=self.assign_reduce,args=(self.reduce_nodelist, reduce_node, node2, node))
                                            th.start()
                                    flag = False
                                    break
                    c.close()
               
    def map(self):
        global result, mutex
        result = {}
        mutex = threading.Lock()
        all_keys = []
        self.map_d = {}
        for node in self.nodelist:
            print node
            c = zerorpc.Client(timeout = 3)
            c.connect('tcp://' + node)
            keys = []
            try:
                keys = c.get_keys()
            except:
                pass
            c.close()
            print keys
            for key in keys:
                if key not in all_keys:
                    all_keys.append(key)
        #print "all:" + all_keys.__str__()
        if len(all_keys) < len(self.map_nodelist):
            for (i, key) in enumerate(all_keys):
                self.map_d[self.map_nodelist[i]] = [all_keys[i]]
        else:
            bucket_size = len(all_keys) / len(self.map_nodelist)
            for (i, node) in enumerate(self.map_nodelist):
                if i < len(self.map_nodelist) - 1:
                    self.map_d[node] = all_keys[i * bucket_size : (i + 1) * bucket_size]
                else:
                    self.map_d[node] = all_keys[i * bucket_size : ]
        print self.map_d
        mapthreads = []
        for node in self.map_nodelist:
            mapthreads.append(threading.Thread(target=self.assign_map,args=(node, self.map_d)))
        
        for t in mapthreads:
            t.start()
        #for t in mapthreads:
        #    t.join()
        
            
    def reduce(self, addr):
        reducethreads = []
        for node in self.reduce_nodelist:
            self.reduce_d2[node] = addr
            reducethreads.append(threading.Thread(target=self.assign_reduce,args=(self.reduce_nodelist, node, addr, node)))
        for t in reducethreads:
            t.start()
        #for t in reducethreads:
        #    t.join()
        
            
        
    def assign_map(self, node, d):
        c = zerorpc.Client(timeout = 20)
        c.connect('tcp://' + node)
        print node + " start mapping"
        self.map_workers[node] = "Working"
        try:
            c.startMap(d[node])
            print node + " finish mapping !!!"
            self.map_workers[node] = "Idle"
#            self.map_d.pop(node)
            self.reduce(node)
        except zerorpc.TimeoutExpired:
            pass
                
        c.close()
        
        
        
    def assign_reduce(self, reduce_nodelist, node, addr, newNode):
        global result, mutex
        #print dict[node] + " start reducing"
        c = zerorpc.Client(timeout = 10)
        c.connect('tcp://' + newNode)
        d = {}
        self.reduce_workers[node] = "Working"
        try:
            d = c.startReduce(reduce_nodelist, addr, node)
            print newNode + " finish reducing !!!"
        except:
            pass 
        c.close()
        
        self.reduce_workers[node] = "Idle"
#        self.reduce_d.pop(node)   
        mutex.acquire()
        self.reduceComplete = self.reduceComplete + 1
        for k in d.keys():
            if k in result.keys():
                result[k] = result[k] + d[k]
            else:
                result[k] = d[k]
        print self.reduceComplete

        if self.reduceComplete == len(self.reduce_workers.keys()) * self.mapNum:
            self.endtime = int(round(time.time() * 1000))
            print "map reduce time: " + str(self.endtime - self.starttime)
            print result
        mutex.release()
        
if __name__ == '__main__':
    addr = '127.0.0.1:29009'
    m = Master(addr)
    s = zerorpc.Server(m)
    s.bind('tcp://' + addr)
    gevent.spawn(m.start)
    s.run()