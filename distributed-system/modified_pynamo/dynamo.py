"""Implementation of Dynamo

Final iteration: add use of vector clocks for metadata"""
import copy
import re
import random
import logging
import sys
import pickle
import time
import zerorpc
from gevent import monkey
monkey.patch_all()
from threading import Thread
import logconfig
from node import Node
from timer import TimerManager
from framework import Framework
from hash_multiple import ConsistentHashTable
from dynamomessages import ClientPut, ClientGet, ClientPutRsp, ClientGetRsp
from dynamomessages import PutReq, GetReq, PutRsp, GetRsp
from dynamomessages import DynamoRequestMessage
from dynamomessages import PingReq, PingRsp
from merkle import MerkleTree
from vectorclock import VectorClock
import gevent
import leveldb

logconfig.init_logging()
_logger = logging.getLogger('dynamo')


# PART dynamonode
class DynamoNode(Node):
    timer_priority = 20
    T = 10  # Number of repeats for nodes in consistent hash table
    N = 2  # Number of nodes to replicate at
    W = 1  # Number of nodes that need to reply to a write operation
    R = 1  # Number of nodes that need to reply to a read operation
    nodelist = []
    chash = ConsistentHashTable(nodelist, T)

    def __init__(self, addr, config_file='server_config'):
        super(DynamoNode, self).__init__()
        self.framework = Framework()
        self.m_addr = "127.0.0.1:29009"
        self.local_store = MerkleTree()  # key => (value, metadata)
        self.pending_put_rsp = {}  # seqno => set of nodes that have stored
        self.pending_put_msg = {}  # seqno => original client message
        self.pending_get_rsp = {}  # seqno => set of (node, value, metadata) tuples
        self.pending_get_msg = {}  # seqno => original client message
        # seqno => set of requests sent to other nodes, for each message class
        self.pending_req = {PutReq: {}, GetReq: {}}
        self.failed_nodes = []
        self.pending_handoffs = {}
        self.result = {}
        self.reduceResult = {}
        self.MapReduceDB = leveldb.LevelDB('./' + addr + '_mrdb')
        self.mapDict = {}
        # Rebuild the consistent hash table
        self.addr = addr
        self.servers = []
        self.db = leveldb.LevelDB('./' + addr + '_db')
        c = zerorpc.Client(timeout=3)
        c.connect('tcp://' + self.m_addr)
        try:
            
            c.add_node(self.addr)
            c.close()
        except:
            pass

        self.pool = gevent.pool.Group()
        self.pool.spawn(self.retry_failed_node)
        self.framework.setDynamo(self)

    def getNodeList(self, servers):
        print servers
        for i, server in enumerate(list(servers)):
            DynamoNode.nodelist.append(server)
        DynamoNode.chash = ConsistentHashTable(DynamoNode.nodelist, DynamoNode.T)
        
# PART reset
    @classmethod
    def reset(cls):
        cls.nodelist = []
        cls.chash = ConsistentHashTable(cls.nodelist, cls.T)

# PART storage
    def store(self, key, value, metadata):
        self.db.Put(key,pickle.dumps((value,metadata)))

    def retrieve(self, key):
        try:
            return pickle.loads(self.db.Get(key))
        except:
            return (None, None)
    
    def are_you_there(self):
        return
# PART retry_failed_node
    def retry_failed_node(self):  # Permanently repeating timer       
        while True:
            if self.failed_nodes:
                for node in self.failed_nodes:
                    c = zerorpc.Client(timeout=1)
                    c.connect('tcp://' + str(node))
                    try:
                        c.are_you_there()
                        c.close()
                        self.recovery(node)
                    except:
                        continue
            time.sleep(1)
        
    def recovery(self, node):
        # Remove all instances of recovered node from failed node list
        recovered_node = node
        while recovered_node in self.failed_nodes:
            self.failed_nodes.remove(recovered_node)
        if recovered_node in self.pending_handoffs:
            for key in self.pending_handoffs[recovered_node]:
                # Send our latest value for this key
                (value, metadata) = self.retrieve(key)
                putmsg = PutReq(self.addr, recovered_node, key, value, metadata)
                self.framework.send_message(putmsg)
                self.framework.schedule()
            del self.pending_handoffs[recovered_node]


    def retry_request(self, reqmsg):
        self.failed_nodes.append(reqmsg.to_node)
        if not isinstance(reqmsg, DynamoRequestMessage):
            return
        # Send the request to an additional node by regenerating the preference list
        preference_list = DynamoNode.chash.find_nodes(reqmsg.key, DynamoNode.N, self.failed_nodes)[0]
        kls = reqmsg.__class__
        # Check the pending-request list for this type of request message
        if kls in self.pending_req and reqmsg.msg_id in self.pending_req[kls]:
            for node in preference_list:
                try:
                    if node not in [req.to_node for req in self.pending_req[kls][reqmsg.msg_id]]:
                        # Found a node on the new preference list that hasn't been sent the request.
                        # Send it a copy
                        newreqmsg = copy.copy(reqmsg)
                        newreqmsg.to_node = node
                        self.pending_req[kls][reqmsg.msg_id].add(newreqmsg)
                        self.framework.send_message(newreqmsg)
                        self.framework.schedule()
                except:
                    pass
                    
# PART rcv_clientput
    def rcv_clientput(self, msg):
        preference_list, avoided = DynamoNode.chash.find_nodes(msg.key, DynamoNode.N, self.failed_nodes)
        non_extra_count = DynamoNode.N - len(avoided)
        # Determine if we are in the list
        if self.addr not in preference_list:
            # Forward to the coordinator for this key
            _logger.info("put(%s=%s) maps to %s", msg.key, msg.value, preference_list)
            coordinator = preference_list[0]
            self.framework.forward_message(msg, coordinator)
            self.framework.schedule()
        else:
            # Use an incrementing local sequence number to distinguish
            # multiple requests for the same key
            seqno = self.generate_sequence_number()
            _logger.info("%s, %d: put %s=%s", self, seqno, msg.key, msg.value)
            # The metadata for a key is passed in by the client, and updated by the coordinator node.
            metadata = copy.deepcopy(msg.metadata)
            metadata.update(self.name, seqno)
            # Send out to preference list, and keep track of who has replied
            self.pending_req[PutReq][seqno] = set()
            self.pending_put_rsp[seqno] = set()
            self.pending_put_msg[seqno] = msg
            reqcount = 0
            for ii, node in enumerate(preference_list):
                if ii >= non_extra_count:
                    # This is an extra node that's only include because of a failed node
                    handoff = avoided
                else:
                    handoff = None
                # Send message to get node in preference list to store
                putmsg = PutReq(self.addr, node, msg.key, msg.value, metadata, msg_id=seqno, handoff=handoff)
                try:
                    self.pending_req[PutReq][seqno].add(putmsg)
                except:
                    pass
                self.framework.send_message(putmsg)
                self.framework.schedule()
                reqcount = reqcount + 1
                if reqcount >= DynamoNode.N:
                    # preference_list may have more than N entries to allow for failed nodes
                    break

# PART rcv_clientget
    def rcv_clientget(self, msg):
        preference_list = DynamoNode.chash.find_nodes(msg.key, DynamoNode.N, self.failed_nodes)[0]
        # Determine if we are in the list
        #print preference_list
        if self.addr not in preference_list:
            # Forward to the coordinator for this key
            _logger.info("get(%s=?) maps to %s", msg.key, preference_list)
            coordinator = preference_list[0]
            self.framework.forward_message(msg, coordinator)
            self.framework.schedule()
        else:
            seqno = self.generate_sequence_number()
            self.pending_req[GetReq][seqno] = set()
            self.pending_get_rsp[seqno] = set()
            self.pending_get_msg[seqno] = msg
            reqcount = 0
            for node in preference_list:
                getmsg = GetReq(self.addr, node, msg.key, msg_id=seqno)
                try:
                    self.pending_req[GetReq][seqno].add(getmsg)
                except:
                    pass
                self.framework.send_message(getmsg)
                self.framework.schedule()
                reqcount = reqcount + 1
                if reqcount >= DynamoNode.N:
                    # preference_list may have more than N entries to allow for failed nodes
                    break

# PART rcv_put
    def rcv_put(self, putmsg):
        _logger.info("%s: store %s=%s", self, putmsg.key, putmsg.value)
        self.store(putmsg.key, putmsg.value, putmsg.metadata)
        if putmsg.handoff is not None:
            for failed_node in putmsg.handoff:
                if failed_node not in self.failed_nodes:
                    self.failed_nodes.append(failed_node)
                if failed_node not in self.pending_handoffs:
                    self.pending_handoffs[failed_node] = set()
                self.pending_handoffs[failed_node].add(putmsg.key)
        putrsp = PutRsp(putmsg)
        self.framework.send_message(putrsp)
        self.framework.schedule()

# PART rcv_putrsp
    def rcv_putrsp(self, putrsp):
        seqno = putrsp.msg_id
        if seqno in self.pending_put_rsp:
            self.pending_put_rsp[seqno].add(putrsp.from_node)
            
            if len(self.pending_put_rsp[seqno]) >= DynamoNode.W:
                _logger.info("%s: written %d copies of %s=%s so done", self, DynamoNode.W, putrsp.key, putrsp.value)
                #_logger.debug("  copies at %s", [node.name for node in self.pending_put_rsp[seqno]])
                # Tidy up tracking data structures
                original_msg = self.pending_put_msg[seqno]
                del self.pending_req[PutReq][seqno]
                del self.pending_put_rsp[seqno]
                del self.pending_put_msg[seqno]
                # Reply to the original client
                client_putrsp = ClientPutRsp(original_msg, putrsp.metadata)
                self.framework.send_message(client_putrsp)
                self.framework.schedule()
        else:
            pass  # Superfluous reply

# PART rcv_get
    def rcv_get(self, getmsg):
        _logger.info("%s: retrieve %s=?", self, getmsg.key)
        (value, metadata) = self.retrieve(getmsg.key)
        getrsp = GetRsp(getmsg, value, metadata)
        self.framework.send_message(getrsp)
        self.framework.schedule()

# PART rcv_getrsp
    def rcv_getrsp(self, getrsp):
        seqno = getrsp.msg_id
        if seqno in self.pending_get_rsp:
            self.pending_get_rsp[seqno].add((getrsp.from_node, getrsp.value, getrsp.metadata))
            #print len(self.pending_get_rsp[seqno])
            if len(self.pending_get_rsp[seqno]) >= DynamoNode.R:
                _logger.info("%s: read %d copies of %s=? so done", self, DynamoNode.R, getrsp.key)
                #_logger.debug("  copies at %s", [(node.name, value) for (node, value, _) in self.pending_get_rsp[seqno]])
                # Coalesce all compatible (value, metadata) pairs across the responses
                results = VectorClock.coalesce2([(value, metadata) for (node, value, metadata) in self.pending_get_rsp[seqno]])
                # Tidy up tracking data structures
                original_msg = self.pending_get_msg[seqno]
                del self.pending_req[GetReq][seqno]
                del self.pending_get_rsp[seqno]
                del self.pending_get_msg[seqno]
                # Reply to the original client, including all received values
                client_getrsp = ClientGetRsp(original_msg,
                                             [value for (value, metadata) in results],
                                             [metadata for (value, metadata) in results])
                self.framework.send_message(client_getrsp)
                #Framework.schedule(timers_to_process=0)
                self.framework.schedule()
        else:
            pass  # Superfluous reply

# PART rcvmsg
    def rcvmsg(self, msg):
        msg = pickle.loads(msg)
        if isinstance(msg, ClientPut):
            #print 'get ClientPut'
            self.rcv_clientput(msg)
        elif isinstance(msg, PutReq):
            #print 'get PutReq'
            self.rcv_put(msg)
        elif isinstance(msg, PutRsp):
            #print 'get PutRsp'
            self.rcv_putrsp(msg)
        elif isinstance(msg, ClientGet):
            #print 'get ClientGet'
            self.rcv_clientget(msg)
        elif isinstance(msg, GetReq):
            #print 'get GetReq'
            self.rcv_get(msg)
        elif isinstance(msg, GetRsp):
            #print 'get GetRsp'
            self.rcv_getrsp(msg)
        elif isinstance(msg, ClientGetRsp):
            print msg.value
            self.mapDict[msg.key] = msg.value[0]
        else:
            raise TypeError("Unexpected message type %s", msg.__class__)

# PART get_contents
    def get_contents(self):
        results = []
        for key, value in self.local_store.items():
            results.append("%s:%s" % (key, value[0]))
        return results
    
    def put_message(self, fromnode, key, value, metadata):
        #print 'client put!!!'
        metadata = pickle.loads(metadata)
        if metadata is None:
            metadata = VectorClock()
        else:
            # A Put operation always implies convergence
            metadata = VectorClock.converge(metadata)
        putmsg = ClientPut(fromnode, self.addr, key, value, metadata)
        self.rcv_clientput(putmsg)
        #self.framework.send_message(putmsg)
       # Framework.schedule(timers_to_process=0)
        #self.framework.schedule()
        
    def get_message(self, fromnode, key):
        #print 'client get!!!'
        getmsg = ClientGet(fromnode, self.addr, key)
        #print '++++++++++' + str(key) + '+++++++++++++++++'
        self.rcv_clientget(getmsg)
        #self.framework.send_message(getmsg)
        #self.framework.schedule()
        #Framework.schedule(timers_to_process=0)

    def get_keys(self):
        keys = []
        for (key, value) in self.db.RangeIter():
            keys.append(key)
        return keys
        
    def startMap(self, keys):
        self.mapDict.clear()
        self.result.clear()
        print 'start mapping'
        for (k,v) in self.MapReduceDB.RangeIter():
            self.MapReduceDB.Delete(k)
        #print keys
        for key in keys:
            self.get_message(self.addr, key)
        #print 'get key finish'
        while True:
            if len(self.mapDict.keys()) == len(keys):
                break
#            gevent.sleep(0.5)
        #print self.mapDict
        #self.count(self.mapDict)
        for key in self.mapDict.keys():
            self.map(key, self.mapDict[key])
        for k in self.result:
            self.MapReduceDB.Put(k,pickle.dumps(self.result[k]))
        return
            
        
        
    def startReduce(self, reduce_nodelist, addr, node):
        print 'start reducing'
        #print addr
        #print dict
        d = {}
        
        print node
        print self.addr
        if addr != self.addr:
            c = zerorpc.Client(timeout = 10)
            c.connect('tcp://' + addr)
            
            try:
                
                d = c.send_to_reduce(reduce_nodelist, node)
                #print 'return'
                #print d
            except:
                pass
            c.close()
        else:
            d = self.send_to_reduce(reduce_nodelist, node)   
            #print 'return'
            #print d 
        self.reduceResult.clear()
        for k in d:
            #print k
            #print len(pickle.loads(d[k]))
            self.reduce(k, pickle.loads(d[k]))
        return self.reduceResult
    
    def reduce(self, word, iterator):
        self.reduceResult[word] = len(iterator)

    def emit(self, word, sum):
        if word not in self.result.keys():
            self.result[word] = [sum]
        else:
            tmplist = []
            tmplist = self.result[word]
            tmplist.append(sum)
            self.result[word] = tmplist
  
    def map(self, name, document):
        result = {}
        #for k in d:
        content = document.__str__().lower()
        #print 'content is: ' + content
        words = content.split()
        for word in words:
            self.emit(word, 1)
    
    


    def send_to_reduce(self, reduce_nodelist, remoteaddr):
        print 'sent------------------------'
        chash = ConsistentHashTable(reduce_nodelist, DynamoNode.T)
        d = {}
        print 'remote: ' + remoteaddr
        #l = []
        for (k,v) in self.MapReduceDB.RangeIter():
            
            print chash.find_nodes(k)[0][0]
            if remoteaddr == chash.find_nodes(k)[0][0]:
                print k
                print pickle.loads(self.MapReduceDB.Get(k))
                d[k] = self.MapReduceDB.Get(k)
                #l.append(k)
        #for k in l:
            #self.MapReduceDB.Delete(k)
        return d
    
if __name__ == '__main__':
    
    addr = sys.argv[1]
    
    bs = DynamoNode(addr)
    s = zerorpc.Server(bs)
    s.bind('tcp://' + addr)
    s.run()