import sys, math, re
import cPickle as pickle
import readARFF

### takes as input a list of attribute values. Returns a float
### indicating the entropy in this data.

def entropy(data) :
    values = set(data)
    e = 0.0
    for value in values :
        p = float(data.count(value)) / len(data)
        e += -1 * p * math.log(p,2)
    return e

### remainder - this is the amount of entropy left in the data after
### we split on a particular attribute. Let's assume the input is of
### the form: [(value1, class1), (value2, class2), ..., (valuen,
### classn)]
def remainder(data) :
    possibleValues = set([item[0] for item in data])
    r = 0.0
    for value in possibleValues :
        c = [item[0] for item in data].count(value)  
        r += (float(c) / len(data) ) * entropy([item[1] for item in
                                                data if item[0] == value])
    return r

### choose the index of the attribute in the current dataset that
### minimizes the remainder. 
### data is in the form [[a1, a2, ..., c1], [b1,b2,...,c2], ... ]
### where the a's are attribute values and the c's are classifications.
### and attributes is a list [a1,a2,...,an] of corresponding attribute values
def selectAttribute(data, attributes) :
    l = {}
    remainderList = []
    for i in range(0, len(attributes)) :
        l[i] = [(item[i],item[-1]) for item in data]
    for i in range(0, len(attributes)) :
        remainderList.append(remainder(l[i]))
        
    min = remainderList[0]
    minIndex = 0
    for i in range(1, len(remainderList)) :
        if remainderList[i] < min :
            min = remainderList[i]
            minIndex = i
    return minIndex
            
    
    
    
    
    
    
    
### a TreeNode is an object that has either:
### 1. An attribute to be tested and a set of children; one for each possible 
### value of the attribute.
### 2. A value (if it is a leaf in a tree)

class TreeNode :
    def __init__(self, attribute, value) :
        self.attribute = attribute
        self.value = value
        self.children = {}

    def __repr__(self) :
        if self.attribute :
            return self.attribute
        else :
            return self.value

    ### a node with no children is a leaf
    def isLeaf(self) :
        return self.children == {}

    ### the input will be:
    ### data - an object to classify - [v1, v2, ..., vn]
    ### the attribute dictionary

    def classify(self, data, attributes, defaultValue) :
        if self.isLeaf() :
            return self.value
        else :
            for item in attributes.keys() :
                if self.attribute in attributes[item].keys() :
                    for v in attributes[item][self.attribute] :
                        if v in data :
                            self.classify(self.children[v], data, attributes, defaultValue)
                    return defaultValue
### a tree is simply a data structure composed of nodes. The root of the tree 
### is itself a node, so we don't need a separate 'Tree' class. We
### just need a function that takes in a dataset and our attribute dictionary,
### builds a tree, and returns the root node.
### makeTree is a recursive function. Our base case is that our
### dataset has entropy 0 - no further tests have to be made. There
### are two other degenerate base cases: when there is no more data to
### use, and when we have no data for a particular value. In this case
### we use either default value or majority value.
### The recursive step is to select the attribute that most increases
### the gain and split on that.

### assume: input looks like this:
### dataset: [[v1, v2, ..., vn, c1], [v1,v2, ..., c2] ... ]
### attributes: [a1,a2,...,an] }

def makeTree(dataset, alist, attributes, defaultValue) :
    for i in range(0, len(alist)) :
        data = [item[-1] for item in dataset]
        if(entropy(data) == 0) :
            tnode = TreeNode()
            tnode.value = data[0]
            return tnode
    if len(dataset) == 0 :
        tnode = TreeNode()
        tnode.value = defaultValue
        return tnode
    else :
        attrIndex = selectAttribute(dataset, alist)
        tnode = TreeNode()
        tnode.attribute = alist(attrIndex)
        d = {}
        for v in attributes[attrIndex][alist(attrIndex)] :
            subset = [item for item in dataset if item[attrIndex] == v]
            for item in subset :
                del item[attrIndex]
            d[v] = subset
            
        for item in dataset :
            del item[attrIndex]
        del alist[attrIndex]
        for item in attributes :
            del item[attrIndex]
            
        for v in d.keys() :
            tnode.children[v] = makeTree(dataset, alist, attributes, defaultValue)
        return tnode

