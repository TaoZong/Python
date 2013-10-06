import cPickle as pickle
import sys
### build graph: 
### takes as input a file in the form:
## a b dist time
### where a and b are destinations, dist is the distance between them, and 
### time is the time needed to travel between them and constructs a graph.

### This graph should be represented as an adjacency list, and stored as a 
### dictionary, with the key in the dictionary being the source of an edge and 
### the value being a tuple containing the destination, distance, and cost.
### For example:
### g[a] = (b,dist,time)

class graph() :
    def __init__(self, infile=None) :
        self.adjlist = {}
        if infile :
            self.buildGraph(infile)


    ### method to print a graph.
    def __repr__(self) :
        str = ""
        for v in self.adjlist :
            str = str + v + ":\n"
            for v2 in self.adjlist[v] :
                for v3 in v2 :
                    str = str + v3 + " "
                str = str + "\n"
            str = str + "\n"
        return str
    ### helper methods to construct edges and vertices. Use these in buildGraph.
    def createVertex(self, str) :
        name, lat,long = str.split(" ",2)
        lat = lat.split("=")[1]
        long = long.split("=")[1]
        return location(name, lat, long)

    def createEdges(self, str) :
        src, dest, dist, time = str.split(" ",4)
        dist = dist.split("=")[1]
        time = time.split("=")[1]
        e1 = edge(src,dest,dist, time)
        e2 = edge(dest,src,dist, time)
        return e1,e2
    
### method that takes as input a file name and constructs the graph described 
### above.
    def buildGraph(self, infile) :
        
        inputFile = open(infile)
        V = []
        line = inputFile.readline()
        if line.startswith("## vertices") :
            while line :
                line = inputFile.readline()
                if line.startswith("## edges") :
                    break
                if line != "\n" :
                    V.append(self.createVertex(line.split("\n")[0]).name)
            for v in V :
                self.adjlist[v] = []
                     
            while line :
                line = inputFile.readline()
                if line != "" :
                    e1, e2 = self.createEdges(line.split("\n")[0])
                    for v in V :
                        if v == e1.src :
                            self.adjlist[v].append((e1.dest, e1.distance, e1.time))
                        elif v == e2.src :
                            self.adjlist[v].append((e2.dest, e2.distance, e2.time))

    

### this method should compute Dijkstra's algorithm and
### return a graph representing the minimum spanning tree computed by Prim.

    def dijkstra(self, source) :
        D = {}
        P = {}
        print self.adjlist.keys()
        for v in self.adjlist.keys() :
            D[v] = 9999
        D[source] = 0
        Q = self.adjlist.keys()
        while Q :
            u = Q[0]
            for v in Q :
                if D[v] < D[u] :
                    u = v
            if D[u] == 9999 :
                break
            Q.remove(u);
            for (v,dist,time) in self.adjlist[u] :
                alt = D[u] + float(dist.split("km")[0])
                if alt < D[v] :
                    D[v] = alt
                    P[v] = u
                    Q.remove(v)
        return D   
        
        

### classes representing locations and edges

class location() :
    def __init__(self, name, lat, longitude) :
        self.name = name
        self.lat = lat
        self.longitude = longitude
    def __hash__(self) :
        return hash(self.name)
    def __eq__(self, other) :
        return self.name == other.name
    



class edge() :
    def __init__(self, src, dest, distance, time) :
        self.src = src
        self.dest = dest
        self.distance = distance
        self.time = time


### usage: buildGraph {--pfile=outfile} {-d} infile
### if --pfile=outfile is provided, write a pickled version of the graph 
### to outfile. Otherwise, print it to standard output.
### if -d, compute djikstra

if __name__ == '__main__' :
    print sys.argv
    if len(sys.argv) == 4 :
        if sys.argv[1].startswith("--pfile=") :
            outfile = sys.argv[1].split("=")[1]
            if(sys.argv[2] == "-d") :
                g = graph(sys.argv[3])
                source = raw_input("Please input a location name as source for dijkstra algorithm:\n")
                distances = g.dijkstra(source)
                
                outputFile = open(outfile, "w")
                outputFile.write("The source is: " + source + "\n\n")
                for d in distances :
                    outputFile.write("The distance to " + d + " is: " + str(distances[d]) + "km\n")
                outputFile.close()
                
    elif len(sys.argv) == 3 :
        if sys.argv[1].startswith("--pfile=") :
            outfile = sys.argv[1].split("=")[1]
            g = graph(sys.argv[2])
            
            outputFile = open(outfile, "w")
            str = ""
            for v in g.adjlist :
                str = str + v + ":\n"
                for v2 in g.adjlist[v] :
                    for v3 in v2 :
                        str = str + v3 + " "
                    str = str + "\n"
                str = str + "\n"
            outputFile.write(str)
            outputFile.close()
        elif sys.argv[1] == "-d" :
            g = graph(sys.argv[2])
            source = raw_input("Please input a location name as source for dijkstra algorithm:\n")
            distances = g.dijkstra(source)
            print("The source is: " + source + "\n")
            for d in distances :
                print("The distance to " + d + " is: " + str(distances[d]) + "km")
    elif len(sys.argv) == 2 :
        g = graph(sys.argv[1])
        print g