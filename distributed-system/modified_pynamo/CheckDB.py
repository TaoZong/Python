'''
Created on Apr 4, 2013

@author: yaolizheng
'''
import leveldb

count = 0

db = leveldb.LevelDB("127.0.0.1:29000_db")
print '-------------------DB1-------------------'
for c in db.RangeIter():
    count = count + 1
    print '%s ' % (c[0])
print '-------------------DB2-------------------'
db = leveldb.LevelDB("127.0.0.1:29001_db")
for c in db.RangeIter():
    count = count + 1
    print '%s ' % (c[0])
print '-------------------DB3-------------------'
db = leveldb.LevelDB("127.0.0.1:29002_db")
for c in db.RangeIter():
    count = count + 1
    print '%s ' % (c[0])
print '-------------------DB6-------------------'   
db = leveldb.LevelDB("127.0.0.1:29006_db")
for c in db.RangeIter():
    count = count + 1
    print '%s ' % (c[0])
    
print count
