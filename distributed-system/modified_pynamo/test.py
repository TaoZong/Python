'''
Created on Apr 14, 2013

@author: yaolizheng
'''
#import sys
#import random
#import unittest
#import logging
#import zerorpc
#
#from framework import Framework, reset_all
#from node import Node
#from history import History
#import logconfig
#
#import dynamomessages
#import dynamo as dynamo99
#import DynamoClientNode

if __name__ == '__main__':
    #reset_all()
    #dynamo99.DynamoNode.reset()
#    c = zerorpc.Client(timeout=3)
#    c.connect('tcp://127.0.0.1:29003')
    #pref_list = dynamo99.DynamoNode.chash.find_nodes('K1', 5)[0]
    #coordinator = pref_list[0]
        # Send in first get-then-put
    #a.get('K1')
    #Framework.schedule(timers_to_process=0)
    #getrsp = a.last_msg
#    c.put('K1', None, 1)
    #Framework.schedule(timers_to_process=0)
        # Send in second get-then-put
    #c.get('K1')
    #Framework.schedule(timers_to_process=0)
    #getrsp = a.last_msg
    #print getrsp
    #return (a, pref_list)
    x = "100"
    y = 9
    z = float(x) / y
    s = str(z)
    print s == '11.1111111111'
