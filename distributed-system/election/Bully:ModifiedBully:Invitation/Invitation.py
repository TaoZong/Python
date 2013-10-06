'''
Created on Feb 18, 2013

@author: ZT
'''

import logging
import sys

import gevent
import zerorpc


class InvitationElection(object):

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
        
        self.s = "Normal"
        self.c = self.addr
        self.d = None
        self.h = -1
        self.Up = []
        self.g = None
        self.counter = 0
        self.greenletList = []
    
        
    def check(self):
        while True:
            gevent.sleep(2)
            if self.s == "Normal" and self.c == self.addr:
                tempSet = []
                for j in range(0, self.n):
                    if j != self.i:
                        try:
                            ans = self.connections[j].are_you_coordinator(self.addr)
                            #print '(CHECK) [%s] : are_you_coordinator = %s' % (self.servers[j], ans)
                        except zerorpc.TimeoutExpired:
                            print '(CHECK) [%s] : [%s] timeout!' % (self.addr, self.servers[j])
                            continue
                        if ans == "Yes":
                            tempSet.append(j)
                if len(tempSet) == 0:
                    continue
                p = tempSet[len(tempSet) - 1]
                if p > self.i:
                    gevent.sleep(10 * (p - self.i))
                if self.i < self.servers.index(self.c):
                    continue
                merge_greenlet1 = self.pool.spawn(self.merge, tempSet)
                self.greenletList.append(merge_greenlet1)
            
    def timeout(self):
        while True:
            gevent.sleep(2)
            myCoord = self.c
            myGroup = self.g
            if myCoord == self.addr:
                print '(TIMEOUT) [%s] : I am the coordinator.' % self.addr
                continue
            else:
                k = self.servers.index(myCoord)
                try:
                    ans = self.connections[k].are_you_there(self.addr, myGroup)
                    #print '(TIMEOUT) [%s] : are_you_there = %s' % (self.servers[k], ans)
                except zerorpc.TimeoutExpired:
                    print '(TIMEOUT) [%s] : [%s] timeout!' % (self.addr, self.servers[k])
                    self.recovery()
                if ans == "No":
                    self.recovery()
                    
            
    def merge(self, coordinatorSet):
        print '(MERGE) [%s] : start new merge' % (self.addr)
        self.s = "Election"
        
        
        '''CALL STOP ! '''
        if len(self.greenletList) > 0:
            self.greenletList.pop()
        if len(self.greenletList) > 0:
            for g in self.greenletList:
                gevent.kill(g)
            self.greenletList = []
        
        self.counter += 1
        self.g = (self.i, self.counter)
        self.c = self.addr
        tempSet = self.Up
        self.Up = []
        for j in coordinatorSet:
            try:
                self.connections[j].invitation(self.addr, self.g)
                print '(MERGE) [%s] : invites [%s]' % (self.addr, self.servers[j])
            except zerorpc.TimeoutExpired:
                print '(MERGE) [%s] : [%s] timeout!' % (self.addr, self.servers[j])
                continue
        for j in tempSet:    
            try:
                self.connections[j].invitation(self.addr, self.g)
                print '(MERGE) [%s] : invites [%s]' % (self.addr, self.servers[j])
            except zerorpc.TimeoutExpired:
                print '(MERGE) [%s] : [%s] timeout!' % (self.addr, self.servers[j])
                continue        
        #gevent.sleep(1)
        self.s = "Reorganization"
        for j in self.Up:
            try:
                self.connections[j].ready(self.addr, self.g, self.d)
                print '(MERGE) [%s] : finish reorganization' % (self.servers[j])
            except zerorpc.TimeoutExpired:
                print '(MERGE) [%s] : [%s] timeout!' % (self.addr, self.servers[j])
                self.recovery(self.addr)
        self.s = "Normal"
        print '(MERGE) [%s] : I am the new coordinator.' % (self.addr) 
        
    def ready(self, remote_addr, gn, x):
        #logging.debug('[%s] ready() called by %s', self.addr, remote_addr)
        if self.s == "Reorganization" and self.g == gn:
            self.d = x
            self.s = "Normal"
    
    def are_you_coordinator(self, remote_addr):
        #logging.debug('[%s] are_you_coordinator() called by %s', self.addr, remote_addr)
        if self.s == "Normal" and self.c == self.addr:
            return "Yes"
        else:
            return "No"
        
    def are_you_there(self, remote_addr, gn):
        #logging.debug('[%s] are_you_there() called by %s', self.addr, remote_addr)
        if self.g == gn and self.c == self.addr and self.servers.index(remote_addr) in self.Up:
            return "Yes"
        else:
            return "No"
        
    def invitation(self, remote_addr, gn):
        print '(INVITATION) [%s] : invites [%s]' % (remote_addr, self.addr) 
        if self.s != "Normal":
            return
        
        
        '''CALL STOP ! '''   
        if len(self.greenletList) > 0:
            self.greenletList.pop()
        if len(self.greenletList) > 0:
            for g in self.greenletList:
                gevent.kill(g)
            self.greenletList = []
        
        temp = self.c
        tempSet = self.Up
        self.s = "Election"
        self.c = remote_addr
        self.g = gn
        if temp == self.addr:
            for k in tempSet:
                try:
                    self.connections[k].invitation(remote_addr, gn)
                    print '(INVITATION) [%s] : invites [%s].' % (remote_addr, self.servers[k])
                except zerorpc.TimeoutExpired:
                    print '(INVITATION) [%s] : [%s] timeout!' % (self.servers[k], remote_addr)
                    continue
        m = self.servers.index(remote_addr)
        try:
            self.connections[m].accept(self.addr, gn)
            print '(INVITATION) [%s] : accepts [%s]\'s invitation.'% (self.addr, remote_addr) 
        except zerorpc.TimeoutExpired:
            print '(INVITATION) [%s] : [%s] timeout!' % (self.addr, remote_addr)
            self.recovery()
        self.s = "Reorganization"
        
    def accept(self, remote_addr, gn):
        if self.s == "Election" and self.g == gn and self.c == self.addr:
            self.Up.append(self.servers.index(remote_addr))
    
    def recovery(self):
        print '(RECOVERY) [%s] is recovering' % (self.addr)
        self.s = "Election"
        
        
        '''CALL STOP !'''
        if len(self.greenletList) > 0:
            self.greenletList.pop()
        if len(self.greenletList) > 0:
            for g in self.greenletList:
                gevent.kill(g)
            self.greenletList = []
        
        self.counter += 1
        i = self.servers.index(self.addr)
        self.g = (i, self.counter)
        self.c = self.addr
        self.Up = []
        self.s = "Reorganization"
        '''A single node task description is computed and placed in self.d '''
        self.s = "Normal"     

    def start(self):
        self.pool = gevent.pool.Group()
        self.recovery()
        self.timeout_greenlet = self.pool.spawn(self.timeout)
        self.check_greenlet = self.pool.spawn(self.check)
        
        

        
if __name__ == '__main__':
    
    logging.basicConfig(level=logging.DEBUG)
    addr = sys.argv[1]
    if len(sys.argv) > 2:
        config = sys.argv[2]
    else:
        config = 'server_config'
    ie = InvitationElection(addr, config)
    s = zerorpc.Server(ie)
    s.bind('tcp://' + addr)
    ie.start()
    # Start server
    logging.debug('[%s] Starting ZeroRPC Server' % addr)
    s.run()
