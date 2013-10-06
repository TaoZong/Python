'''
Created on 2012-4-25

@author: Tao Zong
'''
import os.path
import re
import string
import math
import sys


def classify(table_bad, table_good, dir) :
    tp = 0
    fp = 0
    fn = 0
    tn = 0

    paths = os.listdir(dir)
    for path in paths :        
        path = dir + "/" + path
        testfiles = os.listdir(path)
        for testfile in testfiles :
            good = 0
            bad = 0
            wordlist = []
            testfile = path + "/" + testfile
            if(os.path.isfile(testfile)) :
                testFile = open(testfile)
                text = testFile.read()
                text = stripper(text)
                words = re.split('\W',string.lower(text))
                for word in words :
                    if word != "" and word not in wordlist :
                        wordlist.append(word)
                testFile.close()
            for word in wordlist :
<<<<<<< .mine
=======
                if word in table_bad.keys() :
                    bad += math.log(table_bad[word])
                else :
                    bad += -100
                if word in table_good.keys() :
                    good += math.log(table_good[word])
>>>>>>> .r142
                else :
                    good += -100
                    
                    good += math.log(table_good[word])
                    
            if "spam" in path :
                if bad > good :
                    tp += 1
                else :
                    fn += 1
            else :
                if bad > good :
                    fp += 1
                else :
                    tn += 1
    if tp > 0 :
        p1 = float(tp) / (tp + fp)
        r1 = float(tp) / (tp + fn)
        f1 = 2 * p1 * r1 / (p1 + r1)
    else :
        f1 = 0
    
    if tn > 0 :
        p2 = float(tn) / (tn + fn)
        r2 = float(tn) / (tn + fp)
        f2 = 2 * p2 * r2 / (p2 + r2)
    else :
        f2 = 0
        
    print "non-spam: f-measure: " + str(f2) + "\n" + "spam: f-measure: " + str(f1)
    
        

def createTable(dir) :
    table_probability_bad = {}
    table_probability_good = {}
    table_good = {}
    table_bad = {}
    paths = os.listdir(dir)
    numberOfSpam = 0
    numberOfNonSpam = 0
    for path in paths :
        path = dir + "/" + path
        table, number = calculate(path)
        pos = path.find("spam")
        if pos == -1 :
            for item in table.keys() :
                if item in table_good.keys() :
                    table_good[item] += table[item]
                else :
                    table_good[item] = table[item]
            numberOfNonSpam += number
        else :
            for item in table.keys() :
                if item in table_bad.keys() :
                    table_bad[item] += table[item]
                else :
                    table_bad[item] = table[item]
            numberOfSpam += number
    print "Training set:\nnon-spam size: " + str(numberOfNonSpam) +" emails\nspam size: " + str(numberOfSpam) + " emails\n"
    for item in table_bad.keys() :
        if item in table_good.keys() :
<<<<<<< .mine
            x = float(table_bad[item]) / numberOfSpam
            y = float(table_good[item]) /  numberOfNonSpam
            table_probability_bad[item] = x / (x + y)
            table_probability_good[item] = y / (x + y)

    return table_probability_bad, table_probability_good
=======
            x = float(table_bad[item]) / numberOfSpam
            y = float(table_good[item]) /  numberOfNonSpam
            table_probability_bad[item] = x / (x + y)
            table_probability_good[item] = y / (x + y)
        else :
            table_probability_bad[item]= 1.0
    for item in table_good.keys() :
        if item not in table_bad.keys() :
            table_probability_good[item] = 1.0

    return table_probability_bad, table_probability_good
>>>>>>> .r142
      
<<<<<<< .mine
def calculate(path) :
    d = {}
=======
def calculate(path) :
    d = {}    
>>>>>>> .r142
    
    infiles = os.listdir(path)
    for infile in infiles :
        wordlist = []
        infile = path + "/" + infile
        if os.path.isfile(infile) :
            inputFile = open(infile)
            text = inputFile.read()
            text = stripper(text)
            words = re.split('\W',string.lower(text))
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
    return d, len(infiles)

def stripper(text) :
    p = re.compile(r'<[^<]*?>')
    newtext = p.sub(' ', text)
    return newtext


if __name__ == '__main__' :
    for item in sys.argv :
        if item.find("--traindir") != -1 :
            dir1 = item.split("=")[-1]
        if item.find("--testdir") != -1:
            dir2 = item.split("=")[-1]
    table1, table2 = createTable(dir1)
    classify(table1, table2, dir2)
