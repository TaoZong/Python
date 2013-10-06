#!/usr/bin/env python

import readARFF
import math
from sets import Set 

### a rule is a conjunction of attributes=value on the left-hand side
### (represented as a dictionary) and a classification on the
### right-hand side.
### e.g. outlook=sunny ^ humidity=high -> playTennis
## you shouldn't need to change this.

class rule :
    def __init__(self, classification) :
        self.leftHandSide = {}
        self.classification = classification

    def __repr__(self) :
        return " ^ ".join(["%s=%s" % (k,v) for (k,v) in
                           self.leftHandSide.items()]) + "->" + self.classification


### takes as input a list of classifications and a target classification and 
### returns a float. You shouldn't need to change this.
def precision(data, classification) :
    return float(data.count(classification)) / len(data) 




### takes as input a list of attribute values. Returns a float
### indicating the entropy in this data. you shouldn't need to change this.
def entropy(data) :
    values = set(data)
    e = 0.0
    for value in values :
        p = float(data.count(value)) / len(data)
        e += -1 * p * math.log(p,2)
    return e


### takes as input a rule, a list of attributes, and a data instance.
### returns true if the left-hand side of the rule matches the data. (the 
### classification is not considered.) You shouldn't need to change this.

def lhsMatch(r, alist, instance) :
  for i in range(len(alist)) :
        attribute = alist[i]
        if attribute in r.leftHandSide :
            rval = r.leftHandSide[alist[i]]
            if instance[i] != rval :
                return False
  return True

### takes as input a rule, a list of attributes, and a data instance.
### returns true if both the left and right-hand sides of the rule match the 
### instance. You shouldn't need to change this.

def ruleMatch(r, alist, instance) :
    if lhsMatch(r,alist, instance) :
        return instance[-1] == r.classification
    else :
        return False


### Learning a decision list. I've provided stubs here for the functions 
### you'll need. 

    ### takes as input our attribute list and dataset.
### While the entropy of the data is greater than 0, iteratively construct a 
### rule by selecting the attribute-value pair that minimizes entropy.
### On each iteration, remove data that does not match the left-hand side of
### the rule. Return the rule.

def learnOneRuleDT(alist, data) :
    value = ''
    attr = ''
    classification = ''
    newdata = []
    r = rule(classification)
    (attr, value) = selectAttribute(alist, data)
    r.leftHandSide[attr] = value
    for i in range(0,len(alist)) :
        if alist[i] == attr :
            index = i
    for item in data :
        if value == item[index] :
            newdata.append(item)
    data = newdata
    l = [item[-1] for item in newdata]
    while(entropy(l) > 0) :
        (attr, value) = selectAttribute(alist, newdata)
        r.leftHandSide[attr] = value
        newdata = []
        for i in range(0,len(alist)) :
            if alist[i] == attr :
                index = i
        for item in data :
            if value == item[index] :
                newdata.append(item)
        data = newdata
        l = [item[-1] for item in newdata]
    for item in data :
        r.classification = item[-1]
    return r
### takes as input our attribute list and dataset.
### return the attribute and value that minimize entropy.
### If there is a tie, pick the pair that has the largest coverage (matches 
### the most instances in the data)

def selectAttribute(alist, data): 
    l = {}
    for i in range(0, len(alist)) :
        l[i] = [(item[i],item[-1]) for item in data]
    d = {}
    for i in range(0, len(alist)) :
        s = set([item1 for (item1,item2) in l[i]])
        for v in s :
            d[v] = []
            for (item1,item2) in l[i] :
                if item1 == v :
                    d[v].append(item2)
    
    min = 2
    minAttr = None
    for item in d :
        tmp = entropy(d[item])
        if tmp < min :
            min = tmp
            minAttr = item
        elif tmp == min and len(d[item]) > len(d[minAttr]) :
            min = tmp
            minAttr = item
    for item in data :
        if minAttr in item :
            for i in range(0, len(item)) :
                if item[i] == minAttr :
                    return (alist[i],minAttr)
### takes as input our attribute list and dataset.
### While we have data to classify, learn a rule and then remove all data
### matched by that rule. Return the list of rules (in order).
def buildDecisionList(alist, data) :
    rulelist = []
    newdata = data
    while len(newdata) > 0 :
        data = newdata
        l = []
        r = learnOneRuleDT(alist, data)
        rulelist.append(r)
        for item in r.leftHandSide.keys() :
            l.append(r.leftHandSide[item])
        newdata = []
        for item in data :
            count = 0
            for v in l :
                if v in item :
                    count += 1
            if count < len(l) :
                newdata.append(item)
    return rulelist
### takes as input a decision list, the attribute list, and a data instance.
### returns the classification of the data.
def DTClassify(dlist, alist, instance, defaultValue) :
    for i in range(0, len(dlist)) :
        result = ruleMatch(dlist[i], alist, instance)
        if result :
            return result
    return defaultValue
### Learning a ruleset. If you're clever, you can probably combine these 
### functions with the ones for the decision list with the appropriate 
### parameterization. It's not required, though.


### takes as input our attribute list and dataset, and a classification.
## While precision < 1, refine a rule and remove data that does not match its 
### left-hand side. Return the rule.
def learnOneRule(alist, data, classification) :
    value = ''
    attr = ''
    newdata = []
    r = rule(classification)
    (attr, value) = selectAttributeWithPrecision(alist, data, classification)
    r.leftHandSide[attr] = value
    for i in range(0,len(alist)) :
        if alist[i] == attr :
            index = i
    for item in data :
        if value == item[index] :
            newdata.append(item)
    data = newdata
    l = [item[-1] for item in newdata]
    while(precision(l, classification) < 1) :
        (attr, value) = selectAttributeWithPrecision(alist, newdata, classification)
        r.leftHandSide[attr] = value
        newdata = []
        for i in range(0,len(alist)) :
            if alist[i] == attr :
                index = i
        for item in data :
            if value == item[index] :
                newdata.append(item)
        data = newdata
        l = [item[-1] for item in newdata]
    return r

### select the attribute-value pair that maximizes precision. 
### return the pair.
### If there is a tie, pick the pair that has the largest coverage (matches 
### the most instances in the data)
def selectAttributeWithPrecision(alist, data, classification) :
    l = {}
    for i in range(0, len(alist)) :
        l[i] = [(item[i],item[-1]) for item in data]
    d = {}
    for i in range(0, len(alist)) :
        s = set([item1 for (item1,item2) in l[i]])
        for v in s :
            d[v] = []
            for (item1,item2) in l[i] :
                if item1 == v :
                    d[v].append(item2)
    
    max = -1
    maxAttr = None
    for item in d :
        tmp = precision(d[item], classification)
        if tmp > max :
            max = tmp
            maxAttr = item
        elif tmp == max and len(d[item]) > len(d[maxAttr]) :
            max = tmp
            maxAttr = item
    for item in data :
        if maxAttr in item :
            for i in range(0, len(item)) :
                if item[i] == maxAttr :
                    return (alist[i],maxAttr)

### takes as input the attribute list and the data. Returns a ruleset.
### For each possible classification, call learnOneRule repeatedly until
### all data of that class are covered.
def buildRuleset(alist, data) :
    ruleset = Set()
    tmp = data
    
    classset = set([item[-1] for item in data])
    for classification in classset :
        newdata = tmp
        while len([item for item in newdata if item[-1]==classification]) > 0 :
            data = newdata
            l = []
            r = learnOneRule(alist, data, classification)
            ruleset.add(r)
            for item in r.leftHandSide.keys() :
                l.append(r.leftHandSide[item])
            newdata = []
            for item in data :
                count = 0
                for v in l :
                    if v in item :
                        count += 1
                if count < len(l) :
                    newdata.append(item)
    return ruleset

### takes as input a decision list, the attribute list, and a data instance.
### returns the classification of the data.
def RulesetClassify(dlist, alist, instance, defaultValue) :
    for item in dlist :
        result = ruleMatch(item, alist, instance)
        if result :
            return result
    return defaultValue

