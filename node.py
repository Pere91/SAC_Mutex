from queue import PriorityQueue
from threading import Thread, Condition
from math import ceil, sqrt
import time
from nodeServer import NodeServer
from nodeSend import NodeSend
from message import Message, Message_type
import config
import logger_config
from datetime import datetime
import random

#LOG_FILE_PATH = f"logs/{datetime.now().strftime("%Y%m%d_%H%M%S")}.log"

LOG_FILE_PATH = "logs/log.log"

flog = logger_config.get_file_logger(LOG_FILE_PATH, logger_config.logging.DEBUG)
clog = logger_config.get_console_logger(logger_config.logging.INFO)

class Node(Thread):
    """
    Represents a Node of the distributed system.

    Attributes:
        id (int): Numerical identifier of the Node.
        port (int): Node's port.
        daemon (bool): Thread's daemon option.
        lamport_ts (int): Lamport timestamp of the last message sent.
        server (NodeServer): Server for handling the incoming messages.
        client (Nodesend): Client for handling message sending.
        collegues (list): Colleagues in the Node's quorum.
        condition (Condition): Condition upon which entering the CS is allowed
    """
    _FINISHED_NODES = 0
    _HAVE_ALL_FINISHED = Condition()

    def __init__(self,id):
        """
        Constructor for class Noed.

        Args:
            id (int): Numerical identifier of the Node.
        """
        Thread.__init__(self)
        self.id = id
        self.port = config.port+id
        self.daemon = True
        self.lamport_ts = 0
        self.__form_colleagues()
        self.server = NodeServer(self) 
        self.server.start()
        self.client = NodeSend(self)
        self.condition = Condition()

    def __form_colleagues(self):
        """
        Form the quorum for the Node. If all nodes are displayed in a NxN
        matrix, row-major ordered by node id, colleagues shall be those on the
        same row or on the same column as the Node. Since Maekawa's algorithm
        requires all quora to have the same number of nodes, there must be a
        number of nodes that makes full rows.
        """

        # The dimension of the matrix rounded op from the square root of the
        # number of nodes.
        num_rows = ceil(sqrt(config.numNodes))
        colleague_matrix = []
        cont = True

        # Form all rows
        for i in range(num_rows):
            if not cont:
                break

            # Form each row
            row = []
            for j in range(num_rows):
                pos = i * num_rows + j

                # When the position reaches the number of nodes, finish
                if  pos >= config.numNodes:
                    cont = False
                    break

                row.append(i * num_rows + j)
            colleague_matrix.append(row)

        # List colleagues from the same row and the same column as the Node
        row_colleagues = [i for row in colleague_matrix for i in row if self.id in row]
        col_colleagues = [i for row in colleague_matrix for i in row if (i % num_rows) == (self.id % num_rows)] # [i for row in colleague_matrix for i in row if (i % num_rows) == (self.id % num_rows) and i != self.id]

        # Create quorum
        self.collegues = []
        for i in row_colleagues:
            if i != self.id:
                self.collegues.append(i)
        for i in col_colleagues:
            self.collegues.append(i)


    def do_connections(self):
        """
        Connect to all other nodes via socket.
        """
        self.client.build_connection()

    def run(self):
        """
        Run simulacrum scenario of multiple accesses to a critical section
        using Maekawa's algorithm for mutual exclusion.
        """
        flog.info("Run Node%i with the follows %s"%(self.id,self.collegues))
        clog.info("Run Node%i with the follows %s"%(self.id,self.collegues))
        self.client.start()

        self.wakeupcounter = 0
        while self.wakeupcounter <= 2: # Termination criteria

            time_offset = random.randint(10, 30)
            time.sleep(time_offset / 10)

            # Send requests to all quorum peers
            req = Message(
                    msg_type=Message_type.REQUEST,
                    src=self.id,
                    ts=self.lamport_ts
                )

            self.client.multicast(req, self.collegues)

            flog.debug("Node_%i send msg: %s"%(self.id, req))
            clog.debug("Node_%i send msg: %s"%(self.id, req))

            # Wait for unanimous grant
            with self.condition:
                while len(self.server.grants_received) < len(self.collegues):
                    self.condition.wait()

            # ENTER CRITICAL SECTION
            flog.info(f"[Node_{self.id}]: Greetings from the critical section!")
            clog.info(f"[Node_{self.id}]: Greetings from the critical section!")
            # EXIT CRITICAL SECTION

            # Send release messages to all quorum peers
            rel = Message(
                    msg_type=Message_type.RELEASE,
                    src=self.id,
                    ts=self.lamport_ts
                )

            self.client.multicast(rel, self.collegues)
            flog.debug("Node_%i send msg: %s"%(self.id, req))
            clog.debug("Node_%i send msg: %s"%(self.id, req))

            # Control iteration 
            self.wakeupcounter += 1 
                
        # Wait for all nodes to finish
        flog.info("Node_%i is waiting for all nodes to finish"%self.id)
        clog.info("Node_%i is waiting for all nodes to finish"%self.id)
        self._finished()

        flog.info("Node_%i DONE!"%self.id)
        clog.info("Node_%i DONE!"%self.id)

    #TODO OPTIONAL you can change the way to stop
    def _finished(self): 
        with Node._HAVE_ALL_FINISHED:
            Node._FINISHED_NODES += 1
            if Node._FINISHED_NODES == config.numNodes:
                Node._HAVE_ALL_FINISHED.notify_all()

            while Node._FINISHED_NODES < config.numNodes:
                Node._HAVE_ALL_FINISHED.wait()