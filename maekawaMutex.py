from node import Node
import config

class MaekawaMutex(object):
    """
    Class that implements and runs Maekawa mutual exclusion algorithm

    Attributes:
        nodes (list): Different nodes that form the system.
    """
    def __init__(self):
        """
        Constructor for class MaekawaMutex.
        """
        self.nodes =[Node(i) for i in range(config.numNodes)]

    def define_connections(self):
        """
        Establishes the connections for each node to all others.
        """
        for node in self.nodes:
            node.do_connections()

    def run(self):
        """
        Starts all nodes as threads and waits for them all to finish.
        """
        self.define_connections()
        for node in self.nodes:
            node.start()

        for node in self.nodes:
            node.join()
