'''
Created on Mar 26, 2013

@author: ZT
'''
import store
import time
if __name__ == '__main__':
    s = store.Store('bass01:2181')
    
    starttime = int(round(time.time() * 1000))
    
    if s.put("1", "a"):
        print "success"
    else:
        print "fail"
    if s.put("2", "b"):
        print "success"
    else:
        print "fail"
    if s.put("3", "c"):
        print "success"
    else:
        print "fail"
    if s.put("4", "d"):
        print "success"
    else:
        print "fail"
    if s.put("5", "e"):
        print "success"
    else:
        print "fail"
    if s.put("6", "f"):
        print "success"
    else:
        print "fail" 
    if s.put("7", "g"):
        print "success"
    else:
        print "fail" 
    if s.put("8", "h"):
        print "success"
    else:
        print "fail" 
    if s.put("9", "i"):
        print "success"
    else:
        print "fail"
    if s.put("10", "j"):
        print "success"
    else:
        print "fail" 

    endtime = int(round(time.time() * 1000))
    print "PUT: " + str(endtime - starttime)
    
    starttime = int(round(time.time() * 1000))
    print s.get("1")
    print s.get("2")
    print s.get("3")
    print s.get("4")
    print s.get("5")
    print s.get("6")
    print s.get("7")
    print s.get("8")
    print s.get("9")
    print s.get("10")
    endtime = int(round(time.time() * 1000))
    
    print  "GET: " + str(endtime - starttime)


    
    