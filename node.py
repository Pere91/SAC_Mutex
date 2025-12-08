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
        queue (PriorityQueue): Stores other nodes' requests based on priority.
        grants_sent (tuple): Highest priority node to which a GRANT is sent.
        grants_received (set): IDs of the nodes that have conceded a GRANT.
        yielded (bool): True if the node has already yielded; False otherwise.
        failed (bool): True if the node has received a FAILED; False otherwise.
        in_CS (bool): True if the node is in the critical section; False otherwise.
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
        self.queue = PriorityQueue()
        self.grants_sent = None
        self.grants_received = set()
        self.yielded = False
        self.failed = False
        self.in_CS = False


    def __queue_tostr(self):
        """
        Converts the contents of a PriorityQueue to a formatted string,
        preserving the order.

        Returns:
            string: formatted string representation of the queue contents
        """
        q = []
        while not self.queue.empty():
            q.append(self.queue.get())
        
        for n in q:
            self.queue.put(n)
        
        return f"\t\tts_{self.lamport_ts}: Queue of Node_{self.id}: {q}"
    

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
        col_colleagues = [i for row in colleague_matrix for i in row if (i % num_rows) == (self.id % num_rows)]

        # Create quorum
        self.collegues = []
        for i in row_colleagues:
            if i != self.id:
                self.collegues.append(i)
        for i in col_colleagues:
            self.collegues.append(i)


    def request_handler(self, msg):
        """
        Handler for REQUEST type messages. Cases:
            
            - This node hasn't sent any GRANT yet -> send GRANT
            - This node has sent a GRANT to some higher priority node -> 
                enqueue requester and send INQUIRE to the higher priority node
            - This node has sent a GRANT to non-higher priority nodes ->
                send FAILED to requester and enqueue it

        Args:
            msg (Message): the Message containing the REQUEST
        """

        # Get the highest priority node that has received a GRANT from this
        if self.grants_sent:
            hp_ts, hp_src = self.grants_sent

            # Reply with a FAILED if it has sent a GRANT to a higher
            # priority node and put the request in the queue.
            if (hp_ts, hp_src) < (msg.ts, msg.src):
                rep = Message(
                        Message_type.FAILED,
                        self.id,
                        msg.src,
                        self.lamport_ts
                    )

                self.client.send_message(rep, msg.src)
                self.queue.put((msg.ts, msg.src))

                flog.debug("Node_%i send msg: %s"%(self.id, rep))
                flog.debug(self.__queue_tostr())
                clog.debug("Node_%i send msg: %s"%(self.id, rep))
                clog.debug(self.__queue_tostr())
                

            # Send an INQUIRE to the highest priority node which has given
            # a GRANT to because the requesting one has more priority.
            else:
                rep = Message(
                        Message_type.INQUIRE,
                        self.id,
                        hp_src,
                        self.lamport_ts,
                        (msg.ts, msg.src)
                    )

                self.client.send_message(rep, hp_src)
                self.queue.put((msg.ts, msg.src))

                flog.debug("Node_%i send msg: %s"%(self.id, rep))
                flog.debug(self.__queue_tostr())
                clog.debug("Node_%i send msg: %s"%(self.id, rep))
                clog.debug(self.__queue_tostr())

        # Reply directly with a GRANT if no other GRANTs have been sent.
        else:
            rep = Message(
                    Message_type.GRANT,
                    self.id,
                    msg.src,
                    self.lamport_ts
                )

            self.client.send_message(rep, msg.src)
            self.grants_sent = (msg.ts, msg.src)

            flog.debug("Node_%i send msg: %s"%(self.id, rep))
            flog.debug(self.__queue_tostr())
            clog.debug("Node_%i send msg: %s"%(self.id, rep))
            clog.debug(self.__queue_tostr())       



    def yield_handler(self, msg):
        """
        Handler for YIELD type messages. Puts the yielding node in the queue
        and removes the grant previously sent to it.

        Args:
            msg (Message): Message containing the YIELD
        """

        # Put the yielding node in the queue
        self.queue.put((msg.ts, msg.src))

        # Clear the grant sent to the yielding node
        if self.grants_sent and (msg.ts, msg.src) == self.grants_sent:
            self.grants_sent = None
        
        # Get the info of the highest priority request in the queue
        if not self.queue.empty():
            q_ts, q_src = self.queue.get()

            # Send a GRANT to the highest priority request in the queue and put
            # the yielding node in the queue.
            rep = Message(
                    Message_type.GRANT,
                    self.id,
                    q_src,
                    self.lamport_ts
                )

            self.client.send_message(rep, q_src)

            flog.debug("Node_%i send msg: %s"%(self.id, rep))
            flog.debug(self.__queue_tostr())
            clog.debug("Node_%i send msg: %s"%(self.id, rep))
            clog.debug(self.__queue_tostr())

            # Update the highest prioriry GRANT sent
            self.grants_sent = (q_ts, q_src)

            flog.debug(f"\t\tHP_GRANT Node_{self.id}: {self.grants_sent}")
            clog.debug(f"\t\tHP_GRANT Node_{self.id}: {self.grants_sent}")


    def release_handler(self, msg):
        """
        Handler for RELEASE type messages. Removes the releasing node from both
        the queue and the list of grants sent, and then sents a GRANT to the
        request at the head of the queue.

        Args:
            msg (Message): message containing the RELEASE
        """

        # Remove the releasing node from the queue and grants_sent
        if self.grants_sent and (msg.ts, msg.src) == self.grants_sent:
            self.grants_sent = None

        new_queue = PriorityQueue()
        while  not self.queue.empty():
            q_ts, q_src = self.queue.get()
            if q_src == msg.src:
                continue
            new_queue.put((q_ts, q_src))
        self.queue = new_queue

        # Sent a GRANT to the request with the highest priority
        if not self.queue.empty():

            q_ts, q_src = self.queue.get()
            rep = Message(
                    Message_type.GRANT,
                    self.id,
                    q_src,
                    self.lamport_ts
                )

            self.client.send_message(rep, q_src)

            flog.debug("Node_%i send msg: %s"%(self.id, rep))
            flog.debug(self.__queue_tostr())
            clog.debug("Node_%i send msg: %s"%(self.id, rep))
            clog.debug(self.__queue_tostr())

            # Update the highest priority GRANT sent
            self.grants_sent = (q_ts, q_src)

            flog.debug(f"\t\tHP_GRANT Node_{self.id}: {self.grants_sent}")
            clog.debug(f"\t\tHP_GRANT Node_{self.id}: {self.grants_sent}")
        
        else:
            self.grants_sent = None
                         

    def inquire_handler(self, msg):
        """
        Handler for INQUIRE type messages. If the node hasn't yet gotten into
        the critical section, replies with a YIELD message.

        Args:
            msg (Message): message containing the INQUIRE
        """
        rep = None

        # If it hasn't got the CS, yield
        with self.condition:
            if not self.in_CS:  
                rep = Message(
                        Message_type.YIELD,
                        self.id,
                        msg.src,
                        self.lamport_ts
                    )
               
                self.yielded = True

                # Clear the yielding node from the grants received
                if msg.src in self.grants_received:
                    self.grants_received.remove(msg.src)

        # Send yield message if created
        if rep:
             self.client.send_message(rep, msg.src)
             flog.debug("Node_%i send msg: %s"%(self.id, rep))
             flog.debug(self.__queue_tostr())
             clog.debug("Node_%i send msg: %s"%(self.id, rep))
             clog.debug(self.__queue_tostr())


    def grant_handler(self, msg):
        """
        Handler for GRANT type messages. Adds the GRANT to its own list and
        clears failed and yielded conditions. Notifies if it has gotten all
        the grants from peers.

        Args:
            msg (Message): message containing the GRANT
        """
        with self.condition:
            self.grants_received.add(msg.src)
            self.yielded = False
            self.failed = False

            if not len(self.grants_received) < len(self.collegues):
                self.condition.notify()


    def failed_handler(self, msg):
        """
        Handler for FAILED type messages. Sets the failed and yielded
        conditions.

        Args:
            msg (Message): message containing the FAILED
        """
        with self.condition:
            self.failed = True
            self.yielded = True    
            


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

        # Members of the quorum that are not this node
        multicast_group = [n for n in self.collegues if n != self.id]

        self.client.start()

        self.wakeupcounter = 0
        while self.wakeupcounter <= 2: # Termination criteria

            # Make nodes start at different times
            min_time = 20
            max_time = ceil(config.numNodes * 7.5) + min_time
            time_offset = random.randint(min_time, max_time)
            time.sleep(time_offset / 10)

            # Send requests to all quorum peers
            self.grants_received.add(self.id)

            req = Message(
                    msg_type=Message_type.REQUEST,
                    src=self.id,
                    ts=self.lamport_ts
                )

            self.client.multicast(req, multicast_group)

            flog.debug("Node_%i send msg: %s"%(self.id, req))
            clog.debug("Node_%i send msg: %s"%(self.id, req))

            # Wait for unanimous grant
            with self.condition:
                while len(self.grants_received) < len(self.collegues):
                    self.condition.wait()
                
                self.in_CS = True
            
            # ENTER CRITICAL SECTION
            flog.info(f"[Node_{self.id}]: Greetings from the critical section!")
            clog.info(f"[Node_{self.id}]: Greetings from the critical section!")
            # EXIT CRITICAL SECTION

            with self.condition:
                self.grants_received.clear()
                self.in_CS = False

            # Send release messages to all quorum peers
            rel = Message(
                    msg_type=Message_type.RELEASE,
                    src=self.id,
                    ts=self.lamport_ts
                )
            
            self.client.multicast(rel, multicast_group)
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


    def _finished(self): 
        """
        Condition upon which nodes finish. All nodes must have completed all
        rounds in the critical section.
        """
        with Node._HAVE_ALL_FINISHED:
            Node._FINISHED_NODES += 1
            if Node._FINISHED_NODES == config.numNodes:
                Node._HAVE_ALL_FINISHED.notify_all()

            while Node._FINISHED_NODES < config.numNodes:
                Node._HAVE_ALL_FINISHED.wait()