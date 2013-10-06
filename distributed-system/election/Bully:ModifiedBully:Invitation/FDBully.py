'''
Created on Feb 21, 2013

@author: ZT
'''

import logging
import sys

import gevent
import zerorpc

class ModifiedBullyElection(object):
    
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
        self.fdlist = []
        
        for i, server in enumerate(self.servers):
            if server == self.addr:
                self.i = i
                self.connections.append(self)
            else:
                c = zerorpc.Client(timeout=10)
                c.connect('tcp://' + server)
                self.connections.append(c)
        
        self.status = None
        self.ldr = None
        self.elid = None
        self.acks = []
        self.nextel = 0
        self.pendack = 0
        self.incarn = 0
        
        
     
    def halt(self, t, j):
        if (self.status == "Norm" and self.ldr < j) or (self.status == "Wait" and self.elid[0] < j):
            self.connections[j].rej(t, self.i)
        else:
            self.halting(t, j)
            
    def halting(self, t, j):
        for k in range(self.i + 1, self.n):
            try :
                self.connections[k].playdead(self.addr)
            except zerorpc.TimeoutExpired:
                None
        self.startFD(self.servers[j])
        self.elid = t
        self.status = "Wait"
        try:
            self.connections[j].ack(t, self.i)
        except zerorpc.TimeoutExpired:
            None
        
    def downsig(self, j):
        if((self.status == "Norm" and j == self.ldr) or (self.status == "Wait" and j == self.elid[0])):
            self.startStage2()
        elif self.status == "Elec2" and j == self.pendack:
            self.continStage2()
    
    def fd(self):
        print self.fdlist
        while True:
            gevent.sleep(2)
            for addr in self.fdlist:
                k = self.servers.index(addr)
                try:
                    self.connections[k].are_you_there(self.addr)
                except zerorpc.TimeoutExpired:
                    print '[%s] : [%s] is down' % (self.addr, addr)
                    self.downsig(k)
            
    def playalive(self, addr):
        if addr not in self.fdlist:
            self.fdlist.append(addr)
        
                
    def playdead(self, addr):
        if addr in self.fdlist:
            self.fdlist.remove(addr)
        
    def are_you_there(self, remote_addr):
        return True
    
    def startFD(self, addr):
        self.fdlist.append(addr)
    
    def stopFD(self, addr):
        if addr in self.fdlist:
            self.fdlist.remove(addr)
        
    
    
    def startStage2(self):
        print '[%s] : start stage2...' % self.addr
        for k in range(self.i + 1, self.n):
            try :
                self.connections[k].playalive(self.addr)
            except zerorpc.TimeoutExpired:
                None
        self.elid = (self.i, self.incarn, self.nextel)
        self.nextel += 1
        self.status = "Elec2"
        self.acks = []
        self.pendack = self.i
        self.continStage2()
    
    def rej(self, t, j):
        if self.status == "Elec2" and j == self.pendack:
            self.continStage2()
            
    def continStage2(self):
        print '[%s] : continue stage2...' % self.addr
        if self.pendack < self.n - 1:
            self.pendack += 1
            self.startFD(self.servers[self.pendack])
            try :
                self.connections[self.pendack].halt(self.elid, self.i)
            except zerorpc.TimeoutExpired:
                None
        else:
            self.ldr = self.i
            self.status = "Norm"
            print '[%s] : I am the coordinator' % self.addr
            for k in self.acks:
                try:
                    self.connections[k].leader(self.elid, self.i)
                    print '[%s] : accept that [%s] is the coordinator' % (self.servers[k], self.addr)
                except zerorpc.TimeoutExpired:
                    None
                
    def ack(self, t, j):
        if self.status == "Elec2" and t == self.elid and j == self.pendack:
            self.acks.append(j)
            self.continStage2()
            
    def leader(self, t, j):
        if self.status == "Wait" and t == self.elid:
            self.ldr = j
            self.status = "Norm"
            print '[%s] : my leader is [%s]' % (self.addr, self.servers[self.ldr])
            for k in self.servers:
                if k != self.addr:
                    self.stopFD(k)
            self.startFD(self.servers[self.ldr])
    
    def check(self):
        while(True):
            gevent.sleep(2)
            if self.status == "Norm" and self.ldr == self.i:
                for k in range(self.i + 1, self.n):
                    try:
                        self.connections[k].normOrNot(self.elid, self.i)
                    except zerorpc.TimeoutExpired:
                        None
    
    def normOrNot(self, t, j):
        if (self.status != "Norm" and j < self.elid[0]) or (self.status == "Norm" and j < self.ldr):
            try:
                self.connections[j].notNorm(t, self.i)
            except zerorpc.TimeoutExpired:
                None
        
    def notNorm(self, t, j):
        if self.status == "Norm" and self.ldr == self.i and self.elid == t:
            self.startStage2()
            
    def recovery(self):
        self.incarn += 1
        self.startStage2()
                
    def start(self):
        self.recovery()
        self.pool = gevent.pool.Group()
        self.check_greenlet = self.pool.spawn(self.check)
        self.fd_greenlet = self.pool.spawn(self.fd)
        
                
if __name__ == '__main__':
    
    logging.basicConfig(level=logging.DEBUG)
    addr = sys.argv[1]
    if len(sys.argv) > 2:
        config = sys.argv[2]
    else:
        config = 'server_config'
    mbe = ModifiedBullyElection(addr, config)
    s = zerorpc.Server(mbe)
    s.bind('tcp://' + addr)
    mbe.start()
    # Start server
    logging.debug('[%s] Starting ZeroRPC Server' % addr)
    s.run()