import cPickle as pickle
import sys
import random
### read in data from an ARFF file and return the following data structures:
### A dict that maps an attribute index to a dictionary mapping attribute names to either:
###    - possible values
###    - the string 'string'
###    - the string 'numeric'
### A list containing all data instances, with each instance stored as a tuple.
def readArff(filehandle) :
    inputFile = open(filehandle)
    attribute_amount = 0
    Attr = {}
    Data = {}
    line = inputFile.readline()
    while line :
        if line.startswith("@attribute") :
            T = []
            T1 = []
            D = {}
            attribute_amount += 1
            attribute = line.split("{")[0].split(" ")[1]
            values = line.split("{")[1].split("}")[0]
            T1 = values.split(",")
            for t in T1 :
                t = t.strip()
                T.append(t)
            D[attribute] = T
            Attr[attribute_amount] = D
            line = inputFile.readline()
        elif line.startswith("@data") :
            data_amount = 0
            while line :
                Data1 = []
                Data2 = []
                line = inputFile.readline()
                Data1 = line.split(",")
                if(len(Data1) == attribute_amount) :
                    data_amount += 1
                    for data in Data1 :
                        data = data.strip()
                        Data2.append(data)
                    Data[data_amount] = Data2
                
        else :
            line = inputFile.readline()
        
            
    return Attr, Data    
        

### this method should replace any missing attribute values with the most common value.
def mostCommonValue(attributes, data) :
    attribute_amount = len(attributes)
    count = {}
    
    for attr in attributes :
        for attr2 in attributes[attr] :
            c = {}
            for attr3 in attributes[attr][attr2] :
                c[attr3] = 0
            count[attr] = c
    d2 = []
    for d in data :
        d2 = data[d]
        for i in range(attribute_amount):
            for att in attributes[i + 1] :
                att2 = attributes[i + 1][att]
                if d2[i] in att2 :
                    count[i+1][d2[i]] += 1
    sub = {}
    for i in range(attribute_amount) :
        for c in count[i+1] :
            sub[i+1] = c
        for c in count[i+1] :
            if count[i+1][c] > count[i+1][sub[i+1]] :
                sub[i+1] = c
                
    d2 = []
    for d in data :
        d2 = data[d]
        for i in range(attribute_amount):
            for att in attributes[i + 1] :
                att2 = attributes[i + 1][att]
                if d2[i] not in att2 :
                    d2[i] = sub[i+1]
    return data
## this method should probabilistically replace any missing attribute
## values. For example, if we are working with "outlook" and 50% of
## values are rainy, 20% are sunny and 30% are overcast, you should
## hoose rainy with p=0.5, sunny with p=0.2, and overcast with p=0.3
def probabilisticReplacement(attributes, data) :
    attribute_amount = len(attributes)
    count = {}
    
    for attr in attributes :
        for attr2 in attributes[attr] :
            c = {}
            for attr3 in attributes[attr][attr2] :
                c[attr3] = 0
            count[attr] = c
    d2 = []
    for d in data :
        d2 = data[d]
        for i in range(attribute_amount):
            for att in attributes[i + 1] :
                att2 = attributes[i + 1][att]
                if d2[i] in att2 :
                    count[i+1][d2[i]] += 1
    for c in count :
        total = 0
        sum = 0
        for c2 in count[c] :
            total += count[c][c2]
        for c2 in count[c] :
            sum = sum + float(count[c][c2])
            count[c][c2] = sum / float(total)
    
    d2 = []
    for d in data :
        d2 = data[d]
        for i in range(attribute_amount):
            for att in attributes[i + 1] :
                att2 = attributes[i + 1][att]
                if d2[i] not in att2 :
                    seed = random.random()
                    for c2 in count[i+1] :
                        if seed <= count[i+1][c2] :
                            d2[i] = c2
                            break
    return data
                        
    
### Usage: readARFF {--pfile=outfile -m -r} infile
### If --pfile=outfile, pickle and store the results in outfile. Otherwise, 
### print them to standard out.
## if -m, use mostCommonValue.
## if -r, use probabilisticReplacement
if __name__ == '__main__' :
    if len(sys.argv) == 2 :
        Attr, Data = readArff(sys.argv[1])
        str = "Attributes:\n"
        for attr in Attr :
            for attr1 in Attr[attr] :
                str = str + attr1 + ": "
                for attr2 in Attr[attr][attr1] :
                    str = str + attr2 + " "
                str += "\n"
        str += "\n\nData:\n"
        for data in Data :
            for data1 in Data[data] :
                str = str + data1 + " "
            str += "\n"
        print str
    elif len(sys.argv) == 3 :
        Attr, Data = readArff(sys.argv[2])
        if sys.argv[1].startswith("--pfile=") :
            outputFile = open(sys.argv[1].split("=")[1],"w")
            str = "Attributes:\n"
            for attr in Attr :
                for attr1 in Attr[attr] :
                    str = str + attr1 + ": "
                    for attr2 in Attr[attr][attr1] :
                        str = str + attr2 + " "
                    str += "\n"
            str += "\n\nData:\n"
            for data in Data :
                for data1 in Data[data] :
                    str = str + data1 + " "
                str += "\n"
            outputFile.write(str)
            outputFile.close()
        elif sys.argv[1] == "-m" :
            Data = mostCommonValue(Attr, Data)
            str = "Attributes:\n"
            for attr in Attr :
                for attr1 in Attr[attr] :
                    str = str + attr1 + ": "
                    for attr2 in Attr[attr][attr1] :
                        str = str + attr2 + " "
                    str += "\n"
            str += "\n\nData:\n"
            for data in Data :
                for data1 in Data[data] :
                    str = str + data1 + " "
                str += "\n"
            print str
        elif sys.argv[1] == "-r" :
            Data = probabilisticReplacement(Attr, Data)
            str = "Attributes:\n"
            for attr in Attr :
                for attr1 in Attr[attr] :
                    str = str + attr1 + ": "
                    for attr2 in Attr[attr][attr1] :
                        str = str + attr2 + " "
                    str += "\n"
            str += "\n\nData:\n"
            for data in Data :
                for data1 in Data[data] :
                    str = str + data1 + " "
                str += "\n"
            print str
    elif len(sys.argv) == 4 :
        Attr, Data = readArff(sys.argv[3])
        if sys.argv[1].startswith("--pfile=") :
            outputFile = open(sys.argv[1].split("=")[1],"w")
            if sys.argv[2] == "-m" :
                Data = mostCommonValue(Attr, Data)
                
            elif sys.argv[2] == "-r" :
                Data = probabilisticReplacement(Attr, Data)
                
                str = "Attributes:\n"
                for attr in Attr :
                    for attr1 in Attr[attr] :
                        str = str + attr1 + ": "
                        for attr2 in Attr[attr][attr1] :
                            str = str + attr2 + " "
                        str += "\n"
                str += "\n\nData:\n"
                for data in Data :
                    for data1 in Data[data] :
                        str = str + data1 + " "
                    str += "\n"
            outputFile.write(str)
            outputFile.close()
        
        
        
        
        
        
        
        
        
