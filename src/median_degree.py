# median_degree in python3.5
# By Brian Cechmanek
# https://www.github.com/BrianCechmanek/InsightDataChallenge2016
# This software is provided with no warranties what-so-ever. 

#IMPORTS
import json
import sys
import os
import statistics
import bisect
import datetime
import re


#GLOBALS    
STARTDIR = os.getcwd()

#SCRIPT STARTUP
# navigate to data
# inFile is set by sys.argv, if none stated, read venmo-trans.txt
if len(sys.argv)>=2:
    inFile = str(sys.argv[1])
    print("Processing " + str(inFile) + " instead")
else:
    inFile = "/venmo_input/venmo-trans.txt" 
   
dataLocation = os.getcwd()
os.chdir(dataLocation)


#FUNCTIONS
def log_error(error, jsObject):
    logdir = dataLocation + "/venmo_output/" + "ErrorLog.txt"
    with open(logdir, 'a') as f:
        f.write(error + str(jsObject) + "\n")

def checkIsValid(jsObject):
    """ checks if a JSON- formatted transactional input is valid. 
        Is called in the main loop, to simulate a steamed API call (rather
        than checking a full .txt file upfront). returns false if it is not valid."""
    # check if any key-value is missing
    for k in jsObject:
        if jsObject[k] == "":
            log_error("missing field error ", jsObject)
            return False
    # check if time field is incorrect formatting
    if not bool(jsObject["created_time"]):
        log_error("no created_time ", jsObject)
        return False
    pattern = re.compile("^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
    if not pattern.match(jsObject["created_time"]):
        log_error("time format error ", jsObject)
        return False    
    # finally, does actor transact with themself
    if jsObject["actor"] == jsObject["target"]:
        log_error("self-transaction error ", jsObject)
        return False
   
    return True

def format_time(timestamp):
    """ returns a formatted datetime Object from a timestamp. """
    formatted_time = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
    return formatted_time

def parseJSON(jsObject):
    """ parses a JSON object (after validity check) into a timestamp, actor, target, tubple."""
    parsed = (format_time(jsObject["created_time"]), jsObject["actor"], jsObject["target"]) 
    return parsed


#GRAPH IMPLEMENTATION
class Graph(object):
    """ Graph implementation of actors and targets for the Venmo
        transactional data. Nodes represent an actor or target, and are
        non-directional. Edges represent the transaction connection.
        Timestamps are not represented here- rather handled in Window. """
    
    def __init__(self, graph_dict={}):
        self.graph_dict = graph_dict

    def add_node(self, actor, target):
        """ adds a node to the graph. Since nodes cannot exist without
            a connection, in this implementation, it also adds the edge
            between both nodes. """
        if actor not in self.graph_dict.keys():
            self.graph_dict[actor] = [target]
        else:
            self.graph_dict[actor].append(target)
        if target not in self.graph_dict.keys():
            self.graph_dict[target] = [actor]
        else:
            self.graph_dict[target].append(actor)

    def handle_transaction(self, actor, target):
        """ handles formated transaction to update the graph. transaction is
            of the form = transaction[0] = actor, transaction[1] = target. """
        self.add_node(actor, target)

    def clean_graph(self, actor, target):
        """ clears 'old' transaction data from the input nodes, symmetrically.
            deletes node from graph if it is empty. """
        try:
            self.graph_dict[actor].remove(target)
            self.graph_dict[target].remove(actor)
        except ValueError:
            log_error("Can't remove " + actor + "or " + target + "from graph.", "")
        
        if not self.graph_dict[actor]:
            try:
                self.graph_dict.pop(actor)
            except KeyError as e:
                log_error("dict.pop error ", e)
        if not self.graph_dict[target]:
            try:
                self.graph_dict.pop(target)
            except KeyError as e:
                log_error("dict.pop error ", e)

    # MEDIAN CALCULATION
    def count_edges(self):
        """ returns a list of each nodes summed edges. """
        count = []
        for node in self.graph_dict:
            count.append(len(self.graph_dict[node]))
        return count

    def get_median(self):
        """ returns the median value of all node summed edges in the graph."""
        return ("%.2f" % statistics.median(self.count_edges()) )

    # DEBUGGING METHODS
    def get_nodes(self):
        """ returns a list of the vertices of the graph. """
        return list(self.graph_dict.keys())

    def show_node(self, node):
        """ returns a particular node and it's edges. """
        return node, self.graph_dict[node]
    
    def get_edges(self):
        """ returns a list of the edges of the graph. """
        edges = []
        for node in self.graph_dict:
            edges.append(self.graph_dict[node])
        return edges


#WINDOW IMPLEMENTATION
class Window(object):
    """ A Window holds the 60 second frame for evaluation. Manages adding and popping
        elements from both the window list, and the Graph dictionary.
        input order = TIMESTAMP, ACTOR, TARGET."""
    
    def __init__(self, window=[(datetime.datetime(1, 1, 1, 1, 1, 1))], nodes=[{}]):
        """ initiate window list, with dummy entry for datetime."""
        self.window = window
        self.nodes = nodes

    def insert(self, timestamp, actor, target, graph):        
        if {actor, target} in self.nodes:
            self.window.pop(self.nodes.index({actor, target}))
            self.nodes.remove({actor, target})
        if timestamp > self.window[-1]:
            self.window.append(timestamp)
        else:
            bisect.insort(self.window, timestamp)            
        self.nodes.insert( self.window.index(timestamp), {actor, target} )
        # push to Graph as well
        graph.handle_transaction(actor, target)

    def pop_out_of_window(self, graph):
        """ pop leading window[] values that don't fit in 60s window."""
        i=0
        while (self.window[i]>(self.window[-1]+datetime.timedelta(seconds=60)) and len(self.window)>1):
            try:
                self.window.pop(i)
                removed = list(self.nodes.pop(i))
                graph.clean_graph(removed[i], removed[1])
            except IndexError as e:
                log_error("Index error : ", e)
 
    # DEBUGGING METHODS        
    def get_timestamps(self):
        return self.window    

    def get_nodes(self):
        return self.nodes

    def get_node_timestamps(self):
        transactions = []
        for item in self.window:
            index = self.window.index(item)
            transactions.append( (item, self.nodes[index]) )
        return transactions


#==============================================================================#


# main loop
def main():
    with open(dataLocation + inFile, 'r') as f:  
        """ Currently reads all data as a full block, and advances line by line in main().
            The streamable implementation would follow similarly by feeding lines into
            main(), making a queue. 
            # sample line of data
        # {"created_time": "2016-03-28T23:23:12Z", "target": "Raffi-Antilian", "actor": "Amber-Sauer"}"""    
        data = f.read()
        
    # format to JSON, and load
    data = data.replace('{',',{')
    data = '[' + data[1:] + ']'
    jsData = json.loads(data)

    #INSTANTIATIONS
    venGraph = Graph()
    venWindow= Window()

    #create output.txt and ErrorLog.txt
    outFile = dataLocation + "/venmo_output/" + "output.txt"
    errFile = dataLocation + "/venmo_output/" + "ErrorLog.txt"
    if not os.path.exists(dataLocation + "/venmo_output/"):
        os.makedirs(dataLocation + "/venmo_output/")
    e = open(errFile, 'w')
    e.write("venmo-trans.txt input data errors \n")
    e.close()
    f = open(outFile, 'w')
        
    #MAIN LOOP    
    for i in range(len(jsData)):
        """ As text is already stored in memory, we loop over lines to mimic streamable information intead."""
        transaction = jsData[i]
        if not checkIsValid(transaction):
            continue
        # create tuple, formatting timestamp to datetime within
        transaction = parseJSON(transaction)
        # add datetime to window (and actor/target to Window.nodes (/graph by slave)
        venWindow.insert(transaction[0], transaction[1], transaction[2], venGraph)
        # update window and graph
        venWindow.pop_out_of_window(venGraph)
        # calculate median
        venGraph.count_edges()
        median = venGraph.get_median()
        # output median
        f.write(median +"\n")

    #SCRIPT SHUTDOWN
    f.close()
    os.chdir(STARTDIR)

if __name__ == "__main__":
    main()
