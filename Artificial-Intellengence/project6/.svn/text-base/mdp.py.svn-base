
### code for representing/solving an MDP
### Most of this is complete. You should only need to complete the valueIteration and policyIteration methods.
import sys
import random
import copy



class State :

    def __init__(self, coordString=None) :
        self.utility = 0
        self.reward = 0.0
        ### an action maps to a list of probability/state pairs
        self.transitions = {}
        self.actions = []
        self.policy = None
        self.coords = coordString
        self.isGoal = False

    def computeEU(self, action) :
        return sum([trans[0] * trans[1].utility 
                    for trans in self.transitions[action]])

    def selectBestAction(self) :
        best = max([(self.computeEU(a), a) for a in self.actions])
        return best[1]

    def __eq__(self, other) :
        return self.coords == other.coords

    def __hash__(self) :
        return self.coords.__hash__()


class Map :
    def __init__(self) :
        self.states = {}
        self.error = 0.01
        self.gamma = 0.8
        self.num = 0

    def getState(self, name) :
        try :
            return self.states[name]
        except KeyError :
            return None

### you do this one.
    def valueIteration(self) :
        self.num += 1
        delta = self.error * (1 - self.gamma) / self.gamma
        newstates = {}
        flag = False
        for coord in self.states.keys() :
            if not self.states[coord].isGoal :
                state = self.states[coord]
                newstate = copy.copy(state)
                newaction = state.selectBestAction()
                max = state.computeEU(newaction)
                newstate.policy = newaction
                newstate.utility = state.reward + self.gamma * max            
                if newstate.utility - state.utility > delta or newstate.utility - state.utility < delta * -1 :
                    flag = True
                newstates[coord] = newstate
            else :
                newstates[coord] = self.states[coord]
        if flag :
            ## update the transitions of newstates
            for coord in newstates :
                for action in newstates[coord].actions :
                    list = newstates[coord].transitions[action]
                    for i in range(0, len(list)) :
                        (p, state2) = list[i]
                        newstates[coord].transitions[action][i] = (p, newstates[state2.coords])
            self.states = copy.copy(newstates)
            self.valueIteration()
            
            
                    
                
             
### you do this one.        
    def policyIteration(self) :
        self.num += 1
        delta = self.error * (1 - self.gamma) / self.gamma
        newstates = {}
        flag = False
        n = 0
        
        for coord in self.states.keys() :
            state = self.states[coord]
            if not state.isGoal :
                tmp = 0.0
                list = state.transitions[state.policy]
                for (p, s) in list :
                    tmp += p * s.utility
                self.states[coord].utility = state.reward + self.gamma * tmp
                newaction = state.selectBestAction()
                if newaction != state.policy :
                    state.policy = newaction
                    n += 1
        if n > 0 :
            self.policyIteration()
    
    def assignRandomPolicies(self) :
        #assign random policies
        for coord in self.states.keys() :
            if not self.states[coord].isGoal :
                self.states[coord].policy = self.states[coord].actions[random.randint(0,3)]
        
    def getMapFromFile(self, fname) :
        with open(fname) as infile :
            for line in infile :
                if line.startswith("#") or len(line) < 2 :
                    pass
                elif line.startswith("gamma") :
                    self.gamma = float(line.split(":")[1])
                elif line.startswith("error") :
                    self.error = float(line.split(":")[1])
                elif line.startswith("reward") :
                    reward = float(line.split(":")[1])
                elif line.startswith("goals") :
                    gs = line.split(":")[1]
                    values = gs.split()
                    for i in range(0,len(values),2) :
                        self.states[values[i]] = State(values[i])
                        self.states[values[i]].isGoal = True
                        self.states[values[i]].utility = float(values[i+1])
                        self.states[values[i]].reward = float(values[i+1])
                ### state transitions
                else :
                    values = line.split()
                    if values[0] not in self.states :
                        self.states[values[0]] = State(values[0])
                        self.states[values[0]].isGoal = False
                        self.states[values[0]].reward = reward
                    action = values[1]
                    self.states[values[0]].actions.append(action)
                    transitions = []
                    for x in values[2:] :
                        prob, name = x.split(":") 
                        if not self.getState(name) :
                            self.states[name] = State(name)
                            self.states[name].isGoal = False
                            self.states[name].reward = reward
                        transitions.append((float(prob), self.getState(name)))
                    self.states[values[0]].transitions[action] = transitions
if __name__ == '__main__' :
    filename = raw_input("Please input a file name:")
    ##filename = "rnGraph"
    m = Map()
    m.getMapFromFile(filename)
    input = raw_input("Input 1 for selecting value iteration.\nInput 2 for selecting policy iteration.\n")
    if input == "1" :
        m.valueIteration()
        print [(s.coords, s.utility, s.policy) for s in m.states.values()]
        print "Iteration : " + str(m.num) + " times.\n"  
    elif input == "2" :
        m.assignRandomPolicies()
        m.policyIteration()
        print [(s.coords, s.utility, s.policy) for s in m.states.values()]
        print "Iteration : " + str(m.num) + " times.\n"
    else :
        print "Sorry, invalid input !"