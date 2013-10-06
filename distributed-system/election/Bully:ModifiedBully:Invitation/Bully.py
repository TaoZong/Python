'''
Created on Feb 16, 2013

@author: ZT
'''

import logging
import sys

import gevent
import zerorpc


class BullyElection(object):

    def __init__(self, addr, config_file='server_config'):
        self.addr = addr

        self.servers = []
        f = open(config_file, 'r')
        for line in f.readlines():
            line = line.rstrip()
            self.servers.append(line)
        print 'My addr: %s' % (self.addr)
        print 'Server list: %s' % (str(self.servers))

        self.n = len(self.servers)

        self.connections = []

        for i, server in enumerate(self.servers):
            if server == self.addr:
                self.i = i
                self.connections.append(self)
            else:
                c = zerorpc.Client(timeout=10)
                c.connect('tcp://' + server)
                self.connections.append(c)
    
        self.s = None
        self.c = None
        self.d = None
        self.h = -1
        self.Up = []
        self.greenletList = []
        
    def are_you_there(self, remote_addr):
        #logging.debug('[%s] are_you_there() called by %s', self.addr, remote_addr)
        return True
    
    def are_you_normal(self, remote_addr):
        #logging.debug('[%s] are_you_normal() called by %s', self.addr, remote_addr)
        if self.s == "Normal":
            return "Yes"
        else:
            return "No"
    
    def halt(self, remote_addr):
        #logging.debug('[%s] halt() called by %s', self.addr, remote_addr)
        self.s = "Election"
        self.h = remote_addr
        
        """CALL STOP !"""
        if len(self.greenletList) > 0:
            self.greenletList.pop()
        if len(self.greenletList) > 0:
            for g in self.greenletList:
                gevent.kill(g)
            self.greenletList = []
        
        
    def newCoordinator(self, remote_addr):
        #logging.debug('[%s] newCoordinator() called by %s', self.addr, remote_addr)
        if self.h == remote_addr and self.s == "Election":
            self.c = remote_addr
            self.s = "Reorganization"
    
    def readyBully(self, remote_addr, x):
        #logging.debug('[%s] ready() called by %s', self.addr, remote_addr)
        if self.c == remote_addr and self.s == "Reorganization":
            self.d = x
            self.s = "Normal"
            
    def election(self, addr):
        print '(ELECTION) [%s] : start new election' % (self.addr)
        if self.i < self.n - 1:
            for j in range(self.i + 1, self.n):
                try:
                    result = self.connections[j].are_you_there(self.addr)
                    #print '(ELECTION) [%s] : are_you_there = %s' % (self.servers[j], result)
                except zerorpc.TimeoutExpired:
                    print '(ELECTION) [%s] : [%s] timeout!' % (self.addr, self.servers[j])
                    continue
            
            
        """CALL STOP !"""
        if len(self.greenletList) > 0:
            self.greenletList.pop()
        if len(self.greenletList) > 0:
            for g in self.greenletList:
                gevent.kill(g)
            self.greenletList = []
                
        self.s = "Election"
        self.h = addr
        self.Up = []
        if self.i > 0:
            for j in range(0, self.i):
                try:
                    self.connections[j].halt(addr)
                    print '(ELECTION) [%s] : halts [%s]' % (addr, self.servers[j])
                except zerorpc.TimeoutExpired:
                    print '(ELECTION) [%s] : [%s] timeout!' % (addr, self.servers[j])
                    continue
                if j not in self.Up:
                    self.Up.append(j)
        self.c = addr
        self.s = "Reorganization"
        for j in self.Up:
            try:
                self.connections[j].newCoordinator(addr)
                print '(ELECTION) [%s] : take [%s] as new coordinator' % (self.servers[j], addr)
            except zerorpc.TimeoutExpired:
                print '(ELECTION) [%s] : [%s] timeout!' % (addr, self.servers[j])
                election_greenlet1 = self.pool.spawn(self.election, addr)
                self.greenletList.append(election_greenlet1)
                return
        for j in self.Up:
            try:
                self.connections[j].readyBully(addr, self.d)
                print '(ELECTION) [%s] : finish reorganization' % (self.servers[j])
            except zerorpc.TimeoutExpired:
                print '(ELECTION) [%s] : [%s] timeout!' % (addr, self.servers[j])
                election_greenlet2 = self.pool.spawn(self.election, addr)
                self.greenletList.append(election_greenlet2)
                return
        self.s = "Normal"
        print '(ELECTION) [%s] : I am the new coordinator.' % (self.addr) 
          
    def recovery(self):
        print '(RECOVERY) [%s] is recovering' % (self.addr)
        self.h = -1
        election_greenlet3 = self.pool.spawn(self.election, addr)
        self.greenletList.append(election_greenlet3)
           
    def check(self):
        while True:
            gevent.sleep(2)
            if self.s == "Normal" and self.c == self.addr:
                for j, server in enumerate(self.servers):
                    if j != self.i:
                        try:
                            result = self.connections[j].are_you_normal(self.addr)
                            #print '(CHECK) [%s] : normal = %s' % (server, result)
                        except zerorpc.TimeoutExpired:
                            print '(CHECK) [%s] : [%s] timeout!' % (self.addr, server)
                            continue
                        if result == "No":
                            election_greenlet4 = self.pool.spawn(self.election, self.addr)
                            self.greenletList.append(election_greenlet4)
                        
    def timeout(self):
        while True:
            gevent.sleep(2)
            if self.s == "Normal" or self.s == "Reorganization":
                k = self.servers.index(self.c)
                try:
                    self.connections[k].are_you_there(self.addr)
                    print '(TIMEOUT) [%s] : my coordinator [%s] is alive.' % (self.addr, self.servers[k])
                except zerorpc.TimeoutExpired:
                    print '(TIMEOUT) [%s] : coordinator down! Start election.' % (self.addr)
                    election_greenlet5 = self.pool.spawn(self.election, self.addr)
                    self.greenletList.append(election_greenlet5)
                

    def start(self):
        self.pool = gevent.pool.Group()
        self.recovery()
        self.check_greenlet = self.pool.spawn(self.check)
        self.timeout_greenlet = self.pool.spawn(self.timeout)
      
if __name__ == '__main__':
    
    logging.basicConfig(level=logging.DEBUG)
    addr = sys.argv[1]
    if len(sys.argv) > 2:
        config = sys.argv[2]
    else:
        config = 'server_config'
    be = BullyElection(addr, config)
    s = zerorpc.Server(be)
    s.bind('tcp://' + addr)
    be.start()
    # Start server
    logging.debug('[%s] Starting ZeroRPC Server' % addr)
    s.run()
