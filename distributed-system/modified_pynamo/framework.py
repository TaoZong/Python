"""Framework code for simulating networks"""
import copy
import logging
from collections import deque

from node import Node
from history import History
from timer import TimerManager
from message import ResponseMessage
import logconfig
import pickle
import zerorpc


logconfig.init_logging()
_logger = logging.getLogger('dynamo')


class Framework(object):
    cuts = []  # List of incommunicado sets of nodes
    queue = deque([])  # queue of pending messages
    pending_timers = {}  # request_message => timer
    #block = []
    
     
    def reset(self):
        self.cuts = []
        self.queue = deque([])
        self.pending_timers = {}
    
     
    #def clearBlock(self, node):
    #    self.block.remove(node)
    
     
    #def setNodes(self, nodes):
    #    self.nodeList = nodes
        
    def setDynamo(self, dynamo):
        self.dynamo = dynamo
     
    def cut_wires(self, from_nodes, to_nodes):
        History.add("announce", "Cut %s -> %s" % ([str(x) for x in from_nodes], [str(x) for x in to_nodes]))
        self.cuts.append((from_nodes, to_nodes))

     
    def reachable(self, from_node, to_node):
        for (from_nodes, to_nodes) in self.cuts:
            if from_node in from_nodes and to_node in to_nodes:
                return False
        return True

     
    def send_message(self, msg, expect_reply=True):
        """Send a message"""
        _logger.info("Enqueue %s->%s: %s", msg.from_node, msg.to_node, msg)
        self.queue.append(msg)
        History.add("send", msg)
        # Automatically run timers for request messages if the sender can cope
        # with retry timer pops
        #if (expect_reply and
        #    not isinstance(msg, ResponseMessage) and
        #    'rsp_timer_pop' in msg.from_node.__class__.__dict__ and
        #    callable(msg.from_node.__class__.__dict__['rsp_timer_pop'])):
        #    self.pending_timers[msg] = TimerManager.start_timer(msg.from_node, reason=msg, callback=Framework.rsp_timer_pop)

     
    def remove_req_timer(self, reqmsg):
        if reqmsg in self.pending_timers:
            # Cancel request timer as we've seen a response
            TimerManager.cancel_timer(self.pending_timers[reqmsg])
            del self.pending_timers[reqmsg]

     
    def cancel_timers_to(self, destnode):
        """Cancel all pending-request timers destined for the given node.
        Returns a list of the request messages whose timers have been cancelled."""
        failed_requests = []
        for reqmsg in self.pending_timers.keys():
            if reqmsg.to_node == destnode:
                TimerManager.cancel_timer(self.pending_timers[reqmsg])
                del self.pending_timers[reqmsg]
                failed_requests.append(reqmsg)
        return failed_requests

     
    def rsp_timer_pop(self, reqmsg):
        # Remove the record of the pending timer
        del self.pending_timers[reqmsg]
        # Call through to the node's rsp_timer_pop() method
        _logger.debug("Call on to rsp_timer_pop() for node %s" % reqmsg.from_node)
        reqmsg.from_node.rsp_timer_pop(reqmsg)

     
    def forward_message(self, msg, new_to_node):
        """Forward a message"""
        _logger.info("Enqueue(fwd) %s->%s: %s", msg.to_node, new_to_node, msg)
        fwd_msg = copy.copy(msg)
        fwd_msg.intermediate_node = fwd_msg.to_node
        fwd_msg.original_msg = msg
        fwd_msg.to_node = new_to_node
        self.queue.append(fwd_msg)
        History.add("forward", fwd_msg)
    
     
    #def updateBlock(self, node):
    #    if node not in self.block:
    #        self.block.append(node)
    
      
    #def getBlock(self):
    #    return self.block
    
    #def setChash(self, c):
    #    self.chash = c
  
    #def setN(self, n):
    #    self.N = n
     
    def schedule(self, msgs_to_process=None, timers_to_process=None):
        """Schedule given number of pending messages"""
        if msgs_to_process is None:
            msgs_to_process = 32768
        if timers_to_process is None:
            timers_to_process = 32768
        while self.queue:
            msg = self.queue.popleft()
            try:
                c = zerorpc.Client(timeout=1)
                c.connect('tcp://' + msg.to_node)
            except zerorpc.TimeoutExpired:
                _logger.info("Drop %s->%s: %s as destination down", msg.from_node, msg.to_node, msg)
                History.add("drop", msg)
                self.dynamo.retry_request(msg)
            if isinstance(msg, ResponseMessage):
                        # figure out the original request this is a response to
                try:
                    reqmsg = msg.response_to.original_msg
                except Exception:
                    reqmsg = msg.response_to
            History.add("deliver", msg)
            m = pickle.dumps(msg)
            try:
                c.rcvmsg(m)
                c.close()
            except:
                print 'time out'
                print msg.to_node
                self.dynamo.retry_request(msg)
               
    def _work_to_do(self):
        """Indicate whether there is work to do"""
        if self.queue:
            return True
        if TimerManager.pending_count() > 0:
            return True
        return False


def reset():
    """Reset all message and other history"""
    Framework.reset()
    TimerManager.reset()
    History.reset()


def reset_all():
    """Reset all message and other history, and remove all nodes"""
    reset()
    Node.reset()