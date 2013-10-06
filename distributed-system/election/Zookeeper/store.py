'''
Created on Mar 22, 2013

@author: ZT
'''

import time
from kazoo.client import KazooClient


class Store(object):
    def __init__(self, addr):
        self.serverlist = []
        f = open('server_config', 'r')
        for line in f.readlines():
            line = line.rstrip()
            self.serverlist.append(line)
        self.server = KazooClient(hosts = addr)
        self.server.start()
        self.num = 0
            
               
    def put(self, key, data, TIMEOUT = 10):
        timeout = 0
        while True:
            if timeout > TIMEOUT:
                if not self.server.exists('/tao/fail'):
                    self.server.create('/tao/fail', b'my_value')
                return False
            if not self.server.exists('/tao/processing'):
                print "Start putting Key: " + key + ", Value: " + data
                if not self.server.exists('/tao/put'):       
                    self.server.create('/tao/put', b'my_value')
                if not self.server.exists('/tao/put' + key): 
                    self.server.create('/tao/put/' + key, value = data)
                else:
                    self.server.set('/tao/put/' + key, data)
                  
                
                while not self.server.exists('/tao/result'):
                    if timeout > TIMEOUT:
                        if not self.server.exists('/tao/fail'):
                            self.server.create('/tao/fail', b'my_value')
                        if self.server.exists('/tao/put'):
                            self.server.delete('/tao/put', recursive = True)
                        return False
                    time.sleep(0.1)
                    timeout = timeout + 0.1
                while True:
                    if timeout > TIMEOUT:
                        if not self.server.exists('/tao/fail'):
                            self.server.create('/tao/fail', b'my_value')
                        if self.server.exists('/tao/put'):
                            self.server.delete('/tao/put', recursive = True)
                        return False
                    tmplist = None
                    addrs = None
                    if self.server.exists('/tao/result'):    
                        addrs = self.server.get_children('/tao/result')
                    if self.server.exists('/tao/election'):
                        tmplist = self.server.get_children('/tao/election')
                    if tmplist and addrs and len(tmplist) == len(addrs):
                        if not self.server.exists('/tao/success'):
                            self.server.create('/tao/success', b'my_value')
                        if self.server.exists('/tao/put'):
                            self.server.delete('/tao/put', recursive = True)
                        return True
                    time.sleep(0.1)
                    timeout = timeout + 0.1
            time.sleep(0.1)
            timeout = timeout + 0.1
        
    
    def get(self, key, TIMEOUT = 10):
        timeout = 0
        while True:
            if timeout > TIMEOUT:
                return False
            if not self.server.exists('/tao/processing'):
                self.num = self.num + 1
                if not self.server.exists('/tao/get'):
                    self.server.create('/tao/get',b"my_value")
                if not self.server.exists('/tao/get/key_' + key):
                    self.server.create('/tao/get/key_' + key, b"my_value")    
                self.server.create('/tao/get/key_' + key + '/' + str(self.num), value = b"my_data") 
        
                
                while not self.server.exists('/tao/get/value_' + key + '/' + str(self.num)):
                    if timeout > TIMEOUT:
                        return False
                    time.sleep(0.1)
                    timeout = timeout + 0.1
                data = self.server.get('/tao/get/value_' + key + '/' + str(self.num))[0]
                self.server.delete('/tao/get/value_' + key + '/' + str(self.num))
                return data
            time.sleep(0.1)
            timeout = timeout + 0.1
            
            