'''
Created on Mar 24, 2013

@author: ZT
'''
import sys
import threading
import leveldb
import time

from kazoo.client import KazooClient

class Server(object):
    def __init__(self, addr):        
        self.zk = KazooClient(hosts = addr)
        self.zk.start()
        if not self.zk.exists('/tao'):
            self.zk.create('/tao', b"my_data")
                    
    def election(self, addr):
        if not self.zk.exists('/tao/election'):
            self.zk.create('/tao/election', b"my_data")
        mypath = self.zk.create('/tao/election/guid-n_', value = addr, ephemeral = True, sequence = True)
        self.i = mypath.rsplit('/tao/election/guid-n_')[1]
        self.C = []
        for path in self.zk.get_children('/tao/election'):
            path = path.encode('ascii','ignore') #unicode to string
            seq = path.rsplit('guid-n_')[1]
            self.C.append(seq)
        self.C.sort()  
        index = self.C.index(self.i)  
        if index == 0:
            print '[%s] : I am the leader.' % addr
            self.zk.set('/tao/election', value = addr)
            self.checkIfExists(self.C[0], addr)
        else:
            j = index - 1
            self.checkIfExists(self.C[j], addr)
    
    def checkIfExists(self, target, addr):
        while self.zk.exists('/tao/election/guid-n_' + target):
            time.sleep(0.1)    
        self.C = []
        for path in self.zk.get_children('/tao/election'):
            path = path.encode('ascii','ignore')
            seq = path.rsplit('guid-n_')[1]
            self.C.append(seq)
        self.C.sort()
        index = self.C.index(self.i)
        if index == 0:
            print '[%s] : I am the leader.' % addr
            self.zk.set('/tao/election', value = addr)
            self.checkIfExists(self.C[0], addr)
        else:
            j = index - 1
            self.checkIfExists(self.C[j], addr)
        
    def put(self, addr):
        while True:
            if self.zk.exists('/tao/election'):
                leader = self.zk.get('/tao/election')[0]
                if leader == addr:
                    if self.zk.exists('/tao/put'):
                        if self.zk.exists('/tao/success'):
                            self.zk.delete('/tao/success')
                        if self.zk.exists('/tao/fail'):
                            self.zk.delete('/tao/fail', recursive = True)                            
                        if not self.zk.exists('/tao/processing'):
                            self.zk.create('/tao/processing', "mydata", ephemeral = True)
                        key = None
                        data = None
                        children = self.zk.get_children('/tao/put')
                        if len(children) == 1:
                            key = children[0].encode('ascii','ignore')
                            data = self.zk.get('/tao/put/' + key)[0]
                            self.zk.delete('/tao/put', recursive = True)
                        if key and data:
                            print "Putting Key: " + key + ", Value: " + data
                            self.db.Put(key, data)
                            if not self.zk.exists('/tao/result'):
                                self.zk.create('/tao/result', b"my_data")
                            if not self.zk.exists('/tao/result/' + addr):
                                self.zk.create('/tao/result/' + addr, b"my_data")
                            if not self.zk.exists('/tao/learn'):
                                self.zk.create('/tao/learn')
                            if not self.zk.exists('/tao/learn/' + key):
                                self.zk.create('/tao/learn/' + key, value = data, ephemeral = True)
                            while True:
                                if self.zk.exists('/tao/fail'):
                                    if self.zk.exists('/tao/result'):
                                        self.zk.delete('/tao/result', recursive = True)
                                    if self.zk.exists('/tao/learn'):
                                        self.zk.delete('/tao/learn', recursive = True)
                                    try:
                                        self.db.Delete(key)
                                    except:
                                        None
                                    if self.zk.exists('/tao/data'):
                                        if self.zk.exists('/tao/data/' + key):
                                            tmpdata = self.zk.get('/tao/data/' + key)[0]
                                            self.db.Put(key, tmpdata)
                                    if not self.zk.exists('/tao/fail/' + addr):
                                        self.zk.create('/tao/fail/' + addr, b"my_data", ephemeral = True)
                                   
                                    while True:
                                        if len(self.zk.get_children('/tao/election')) == len(self.zk.get_children('/tao/fail')):
                                            self.zk.delete('/tao/fail', recursive = True)
                                            if self.zk.exists('/tao/processing'):
                                                self.zk.delete('/tao/processing')
                                            break
                                        time.sleep(0.1)
                                    break
                                
                                elif self.zk.exists('/tao/success'):
                                    if self.zk.exists('/tao/result'):
                                        self.zk.delete('/tao/result', recursive = True)
                                    if self.zk.exists('/tao/learn'):
                                        self.zk.delete('/tao/learn', recursive = True)
                                        
                                    if not self.zk.exists('/tao/data'):
                                        self.zk.create('/tao/data', "mydata")
                                    if self.zk.exists('/tao/data/' + key):
                                        self.zk.set('/tao/data/' + key, data)
                                    else:
                                        self.zk.create('/tao/data/' + key, data)
                                    self.zk.delete('/tao/success')
                                    if self.zk.exists('/tao/processing'):
                                        self.zk.delete('/tao/processing')
                                    break
                                time.sleep(0.1)
                else:
                    if self.zk.exists('/tao/learn'):
                        learners = self.zk.get_children('/tao/learn')
                        if len(learners) == 1:
                            learner_key = learners[0].encode('ascii','ignore')
                            learner_data = self.zk.get('/tao/learn/' + learner_key)[0]
                            if learner_key and learner_data:
                                print "Learning Key: " + learner_key + ", Value: " + learner_data
                                self.db.Put(learner_key, learner_data)
                                if not self.zk.exists('/tao/result'):
                                    self.zk.create('/tao/result', b"my_data")
                                if not self.zk.exists('/tao/result/' + addr):
                                    self.zk.create('/tao/result/' + addr, b"my_data")
                                while True:
                                    if self.zk.exists('/tao/fail'):
                                        self.db.Delete(learner_key)
                                        if self.zk.exists('/tao/data/' + learner_key):
                                            newdata = self.zk.get('/tao/data/' + learner_key)[0]
                                            self.db.Put(learner_key, newdata)
                                        if not self.zk.exists('/tao/fail/' + addr):
                                            self.zk.create('/tao/fail/' + addr, b"my_data", ephemeral = True)
                                        break
                                    elif self.zk.exists('/tao/success'):
                                        break
                                    time.sleep(0.1)
            time.sleep(0.5)
    
            
    def get(self, addr):
        while True:
            time.sleep(0.1)
            if self.zk.exists('/tao/election'):
                leader = self.zk.get('/tao/election')[0]
                if leader == addr:
                    if self.zk.exists('/tao/get'):
                        children = self.zk.get_children('/tao/get')
                        if len(children) > 0:
                            for child in children:
                                child = child.encode('ascii','ignore')
                                if child.startswith('key_'):
                                    key = child.rsplit('key_', 2)[1]
                                    grandchildren = self.zk.get_children('/tao/get/' + child)
                                    if len(grandchildren) > 0:
                                        for grandchild in grandchildren:
                                            grandchild = grandchild.encode('ascii','ignore')
                                            data = None
                                            try:
                                                data = self.db.Get(key)
                                            except:
                                                print "Key: " + key + " not found."
                                            
                                            if data:                                                
                                                num = grandchild
                                                if not self.zk.exists('/tao/get/value_' + key):
                                                    self.zk.create('/tao/get/value_' + key, b"my_data")
                                                if not self.zk.exists('/tao/get/value_' + key + '/' + num):
                                                    self.zk.create('/tao/get/value_' + key + '/' + num, data, ephemeral = True)
                                            self.zk.delete('/tao/get/key_' + key + '/' + grandchild)
                                    
                                
                                
                            
                
    def recovery(self, addr):
        leveldb.DestroyDB('./db')
        self.db = leveldb.LevelDB('./db')
        if self.zk.exists('/tao/data'):
            data_children = self.zk.get_children('/tao/data')
            for key in data_children:
                data = self.zk.get('/tao/data/' + key)[0]
                self.db.Put(key, data)
                
    def start(self, addr):
        self.recovery(addr)
        t1 = threading.Thread(target = self.election, args = (addr,))
        t2 = threading.Thread(target = self.put, args = (addr,))
        t3 = threading.Thread(target = self.get, args = (addr,))
        t1.start()
        t2.start()
        t3.start()               
if __name__ == '__main__':
    addr = sys.argv[1]
    server = Server(addr)
    server.start(addr)
    
    
    
    
    
    
    