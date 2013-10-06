
### This is an abstract base class for the search queues you will implement.

class SearchQueue :
    def __init__(self) :
        self.q = []

    def insert(self, item) :
        pass
    def pop(self) :
        pass
    def isEmpty(self) :
        return self.q == []

### you complete this.
class BFSQueue(SearchQueue) :

### you complete this.
class DFSQueue(SearchQueue) :

### you complete this
class AStarQueue(SearchQueue) :

    ### override the constructor to both call the parent class'
    ##constructor and also store a heuristic function.
    def__init__(self, hfunc) :



### constraints. A constraint is a function that can be applied to a
### path. It returns true if the constraint holds, and false
### otherwise.

def lengthUnderTen(apath) :
    return len(apath.pathSoFar) < 10

def containsGGPark(apath) :
    return len([v for v in apath.pathSoFar if v.name ==
                'GoldenGatePark'])


### length under 10, contains 3 towers

### USF before GG park

### Contains either Chinatown or Alcatraz, but not both



