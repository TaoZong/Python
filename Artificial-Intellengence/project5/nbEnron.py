'''
Created on 2012-4-26

@author: Tao Zong
'''
import os.path
import re
import string
import math
import sys

        
def classify(table1, table2, dir) :
    d1 = {}
    d2 = {}
    classlist = []
    for item in table2.keys() :
        for item2 in table2[item].keys() :
            classlist.append(item + "/" + item2)
    paths = os.listdir(dir)
    for path in paths :
        path = dir + "/" + path
        paths2 = os.listdir(path)
        for path2 in paths2 :
            path2 = path + "/" + path2
            testfiles = os.listdir(path2)
            for testfile in testfiles :
                d = {}
                f = {}
                wordlist = []
                testfile = path2 + "/" + testfile
                if(os.path.isfile(testfile)) :
                    testFile = open(testfile)
                    line = testFile.readline()
                    while not line.startswith("X-FileName") :
                        line = testFile.readline()
                    line = testFile.readline()
                    while line :
                        if line.find("---") != -1 :
                            while line :
                                line = testFile.readline()
                                if line.find("Subject:") != -1 :
                                    break
                        line = testFile.readline()
                        words = re.split('\W',string.lower(line))
                        for word in words :
                            if word not in wordlist :
                                wordlist.append(word)
                for item in table1.keys() :
                    for word in wordlist :
                        if word in table1[item].keys() :
                            if item not in d.keys() :
                                f[item] = float(math.log(table1[item][word]))
                            else :
                                f[item] += float(math.log(table1[item][word]))
                        else :
                            if item not in f.keys() :
                                f[item] = -100
                            else :
                                f[item] += -100
                tmp1 = -9999999
                tmppath1 = ""
                for item in f :
                    if f[item] > tmp1 :
                        tmp1 = f[item]
                        tmppath1 = item
                d1[testfile] = tmppath1
                    
                for item2 in table2.keys() :
                    if item2 == tmppath1 :
                        for item in table2[item2].keys() :
                            for word in wordlist :
                                if word in table2[item2][item].keys() :
                                    if (item2 + "/" + item) not in d.keys() :
                                        d[item2 + "/" + item] = float(math.log(table2[item2][item][word]))
                                    else :
                                        d[item2 + "/" + item] += float(math.log(table2[item2][item][word]))
                                else :
                                    if (item2 + "/" + item) not in d.keys() :
                                        d[item2 + "/" + item] = -100
                                    else :
                                        d[item2 + "/" + item] += -100
                tmp2 = -9999999
                tmppath2 = ""
                for item in d :
                    if d[item] > tmp2 :
                        tmp2 = d[item]
                        tmppath2 = item
                d2[testfile] = tmppath2
    for item in classlist :
        tp = 0
        fp = 0
        fn = 0
        tn = 0
        for item2 in d2.keys() :
            if item2.find(item) != -1 :
                if item == d2[item2] :
                    tp += 1
                else :
                    fn += 1
            else :
                if item == d2[item2] :
                    fp += 1
                else :
                    tn += 1
        if tp > 0 :
            p = float(tp) / (tp + fp)
            r = float(tp) / (tp + fn)
            f = 2 * p * r / (p + r)
        else :
            f = 0     
        print item + ": f-measure: " + str(f)
        
def createTableSender(dir) :
    print "Training Set:"
    paths = os.listdir(dir)
    for path in paths :
        print "\nSender: " + path
        path2 = dir + "/" + path
        d = calculate(path2)
        paths3 = os.listdir(path2)
        for path3 in paths3 :
            path4 = path2 + "/" + path3
            print "Subject: " + path3 + " size: " + str(len(os.listdir(path4))) + " emails"
    print "\n"        
    return createTableSubject(dir)
                
def createTableSubject(dir) :
    table = {}
    table2 = {}
    paths = os.listdir(dir)
    
    for path in paths :
        path2 = dir + "/" + path
        d = calculate(path2)
        table[path] = d
    table2 = {}
    for path in paths:
        table2[path] = {}
    for path in paths :
        for path2 in paths:
            if path2 != path :
                for word in table[path].keys() :
                    table2[path][word] = table[path][word]
                    if word in table[path2].keys() :
                            table2[path][word] += table[path2][word]
    for path in paths :
        for word in table[path].keys() :
            table[path][word] = table[path][word] / table2[path][word]
    return table
        
def calculate(path) :
    d = {}
    flag = False    
    infiles = os.listdir(path)
    for item in infiles :
        item = path + "/" + item
        if os.path.isdir(item) :
            flag = True
    if flag :
        infiles = []
        tmpdirs = os.listdir(path)
        for item in tmpdirs :
            tmppath = path + "/" + item
            tmpfiles = os.listdir(tmppath)
            for tmpfile in tmpfiles :
                infiles.append(item + "/" + tmpfile)
    for infile in infiles :
        wordlist = []
        infile = path + "/" + infile
        if os.path.isfile(infile) :
            inputFile = open(infile)
            line = inputFile.readline()
            while not line.startswith("X-FileName") :
                line = inputFile.readline()
            line = inputFile.readline()
            while line :
                if line.find("---") != -1 :
                    while line :
                        line = inputFile.readline()
                        if line.find("Subject:") != -1 :
                            break
                line = inputFile.readline()
                words = re.split('\W',string.lower(line))
                for word in words :
                    if word not in wordlist :
                        wordlist.append(word)
            for word in wordlist :
                if word != "" :
                    if word not in d.keys() :
                        d[word] = 1
                    else :
                        d[word] += 1                
            inputFile.close()
    for item in d :
        d[item] = float(d[item]) / len(infiles)   
    return d

if __name__ == '__main__' :
    
    for item in sys.argv :
        if item.find("--traindir") != -1 :
            dir1 = item.split("=")[-1]
        if item.find("--testdir") != -1:
            dir2 = item.split("=")[-1]
    table1 = createTableSender(dir1)
    table2 = {}
    dirs = os.listdir(dir1)
    for item in dirs :
        path = dir1 + "/" + item
        table2[item] = createTableSubject(path)
    classify(table1, table2, dir2)
        