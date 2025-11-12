from threading import Event, Thread, Timer, Condition
from datetime import datetime, timedelta
from math import ceil, sqrt
import time
from nodeServer import NodeServer
from nodeSend import NodeSend
from message import Message, Message_type
import config
import random 

class Node(Thread):
    _FINISHED_NODES = 0
    _HAVE_ALL_FINISHED = Condition()

    def __init__(self,id):
        Thread.__init__(self)
        self.id = id
        self.port = config.port+id
        self.daemon = True
        self.lamport_ts = 0

        self.server = NodeServer(self) 
        self.server.start()

        self.__form_colleagues()
        self.client = NodeSend(self) 

    def __form_colleagues(self):
        num_rows = ceil(sqrt(config.numNodes))

        colleague_matrix = []
        cont = True

        for i in range(num_rows):
            if not cont:
                break

            row = []
            for j in range(num_rows):
                pos = i * num_rows + j

                if  pos >= config.numNodes:
                    cont = False
                    break

                row.append(i * num_rows + j)

            colleague_matrix.append(row)

        row_colleagues = [i for row in colleague_matrix for i in row if self.id in row]
        col_colleagues = [i for row in colleague_matrix for i in row if (i % num_rows) == (self.id % num_rows) and i != self.id]

        self.collegues = []

        for i in row_colleagues:
            if i != self.id:
                self.collegues.append(i)
        for i in col_colleagues:
            self.collegues.append(i)

        


    def do_connections(self):
        self.client.build_connection()

    def run(self):
        print("Run Node%i with the follows %s"%(self.id,self.collegues))
        self.client.start()

        
        #TODO MANDATORY Change this loop to simulate the Maekawa algorithm to 
        # - Request the lock
        # - Wait for the lock
        # - Release the lock
        # - Repeat until some condition is met (e.g. timeout, wakeupcounter == 3)

        self.wakeupcounter = 0
        while self.wakeupcounter <= 2: # Termination criteria

            self.server.grants_received.clear()

            # Increase timestamp
            # self.lamport_ts += 1

            # Send requests to all quorum peers
            self.client.multicast(
                Message(
                    msg_type=Message_type.REQUEST,
                    src=self.id,
                    ts=self.lamport_ts
                ),
                self.collegues
            )

            # Wait for unanimous grant
            while len(self.server.grants_received) < len(self.collegues):
                time.sleep(0.01)

            # ENTER CRITICAL SECTION
            print(f"[Node_{self.id}]: Greetings from the critical section!")
            # EXIT CRITICAL SECTION

            # Send release messages to all quorum peers
            self.client.multicast(
                Message(
                    msg_type=Message_type.RELEASE,
                    src=self.id,
                    ts=self.lamport_ts
                ),
                self.collegues
            )

            # Control iteration 
            self.wakeupcounter += 1 
                
        # Wait for all nodes to finish
        print("Node_%i is waiting for all nodes to finish"%self.id)
        self._finished()

        print("Node_%i DONE!"%self.id)

    #TODO OPTIONAL you can change the way to stop
    def _finished(self): 
        with Node._HAVE_ALL_FINISHED:
            Node._FINISHED_NODES += 1
            if Node._FINISHED_NODES == config.numNodes:
                Node._HAVE_ALL_FINISHED.notify_all()

            while Node._FINISHED_NODES < config.numNodes:
                Node._HAVE_ALL_FINISHED.wait()