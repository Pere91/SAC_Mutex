from copy import deepcopy
from datetime import datetime, timedelta
from math import ceil, sqrt
from threading import Event, Thread, Timer
import utils
import config

# DONT MODIFY THIS CLASS
class NodeSend(Thread):
    """
    Handles a node's operations related to message sending.

    Attributes:
        node (Node): Node that sends the messages.
        client_sockets (list): All nodes' sockets as clients.
    """
    def __init__(self, node):
        """
        Constructor for class NodeSend.

        Args:
            node (Node): Node that sends the messages.
        """
        Thread.__init__(self)
        self.node = node
        self.client_sockets = [utils.create_client_socket() for i in range(config.numNodes)]
    
    def build_connection(self):
        """
        Connects each node client socket to the host and port.
        """
        for i in range(config.numNodes):
            self.client_sockets[i].connect(('localhost',config.port+i))
    
    def run(self):
        None

    def send_message(self, msg, dest, multicast=False):
        """
        Sends a message to a single destination.

        Args:
            msg (Message): Message to be sent.
            dest (int): Destination Node id.
            multicast (bool, optional): True for multicast option; False for single destination. Defaults to False.
        """
        if not multicast:
            self.node.lamport_ts += 1
            msg.set_ts(self.node.lamport_ts)
        assert dest == msg.dest
        self.client_sockets[dest].sendall(bytes(msg.to_json(),encoding='utf-8'))


    def multicast(self, msg, group):
        """
        Sends a message to all Nodes within a group.

        Args:
            msg (Message): Message to be sent.
            group (list): IDs of all the nodes in the group.
        """
        self.node.lamport_ts += 1
        msg.set_ts(self.node.lamport_ts)
        for dest in group:
            new_msg = deepcopy(msg)
            new_msg.set_dest(dest)
            assert new_msg.dest == dest
            assert new_msg.ts == msg.ts
            self.send_message(new_msg, dest, True)

