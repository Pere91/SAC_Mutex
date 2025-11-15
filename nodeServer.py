import select
from threading import Thread
import utils
from message import Message, Message_type
import json

from queue import PriorityQueue

class NodeServer(Thread):
    """
    Handles a node's receiving message operations and the responses to them.

    Attributes:
        node (Node): Node that receives the messages as a server.
        daemon (bool): Thread's daemon option.
        queue (PriorityQueue): Stores other nodes' requests based on priority.
        grants_sent (tuple): Highest priority node to which a GRANT is sent.
        grants_received (set): IDs of the nodes that have conceded a GRANT.
        yielded (bool): True if the node has already yielded; False otherwise.
        failed (bool): True if the node has received a FAILED; False otherwise.
        connection_list(list): Stores all connections to this node as server.
        server_socket(socket.socket): Socket as server.
    """
    def __init__(self, node):
        """
        Constructor for class NodeServer.

        Args:
            node (Node): Node that receives the messages as a server.
        """
        Thread.__init__(self)
        self.node = node
        self.daemon = True

        self.queue = PriorityQueue()
        self.grants_sent = None
        self.grants_received = set()
        self.yielded = False
        self.failed = False
    
    def run(self):
        """
        Worker for the objects of this class launched as Threads.
        """
        self.update()

    def update(self):
        """
        Handles the receiving of messages. Parses a stream of bytes to detect
        separate JSONs, then converts them back into Messages so they can be
        processed. 
        """
        self.connection_list = []
        self.server_socket = utils.create_server_socket(self.node.port)
        self.connection_list.append(self.server_socket)

        while self.node.daemon:
            (read_sockets, write_sockets, error_sockets) = select.select(
                self.connection_list, [], [], 5)
            if not (read_sockets or write_sockets or error_sockets):
                print('NS%i - Timed out'%self.node.id) #force to assert the while condition 
            else:
                for read_socket in read_sockets:
                    if read_socket == self.server_socket:
                        (conn, addr) = read_socket.accept()
                        self.connection_list.append(conn)
                    else:
                        try:
                            msg_stream, _ = read_socket.recvfrom(4096)
                            try:
                                # Extract separate JSONs from the byte stream
                                # and convert them back to Messages to be processed.
                                msgs = Message.parse(str(msg_stream, "utf-8"))
                                for m in msgs:
                                    self.process_message(Message.from_json(json.loads(m)))
                            except Exception as e:
                                print("Exception: ", end="")
                                print(e)
                        except:
                            read_socket.close()
                            self.connection_list.remove(read_socket)
                            continue
        
        self.server_socket.close()

    def process_message(self, msg):
        """
        Determines which type of message is received and acts according each
        case.

        Args:
            msg (Message): Message received.

        Raises:
            ValueError: If the type of the Message is not valid.
        """
        print("Node_%i receive msg: %s"%(self.node.id,msg))

        # Received a REQUEST
        if msg.msg_type == Message_type.REQUEST:

            # Get the highest priority node that has received a GRANT from this
            if self.grants_sent:
                hp_ts, hp_src = self.grants_sent

                # Reply with a FAILED if it has sent a GRANT to a higher
                # priority node and put the request in the queue.
                if (hp_ts, hp_src) < (msg.ts, msg.src):
                    self.node.client.send_message(
                        Message(
                            Message_type.FAILED,
                            self.node.id,
                            msg.src,
                            self.node.lamport_ts
                        ),
                        msg.src
                    )
                    self.queue.put((msg.ts, msg.src, msg))

                # Send an INQUIRE to the highest priority node which has given
                # a GRANT to because the requesting one has more priority.
                else:
                    self.node.client.send_message(
                        Message(
                            Message_type.INQUIRE,
                            self.node.id,
                            hp_src,
                            self.node.lamport_ts
                        ),
                        hp_src
                    )

            # Reply directly with a GRANT if no other GRANTs have been sent.
            else:
                self.node.client.send_message(
                    Message(
                        Message_type.GRANT,
                        self.node.id,
                        msg.src,
                        self.node.lamport_ts
                    ),
                    msg.src
                )
                self.grants_sent = (msg.ts, msg.src)

        # Received a YIELD            
        elif msg.msg_type == Message_type.YIELD:

            # Get the info of the highest priority request in the queue
            q_ts, q_src, q_msg = self.queue.get()

            # Send a GRANT to the highest priority request in the queue and put
            # the yielding node in the queue.
            self.node.client.send_message(
                Message(
                    Message_type.GRANT,
                    self.node.id,
                    q_src,
                    self.node.lamport_ts
                ),
                q_src
            )
            self.queue.put((msg.ts, msg.src, msg))

            # Update the highest prioriry GRANT sent if needed
            hp_ts, hp_src = self.grants_sent
            if (q_ts, q_src) < (hp_ts, hp_src):
                self.grants_sent = (q_ts, q_src)

        # Received a RELEASE
        elif msg.msg_type == Message_type.RELEASE:

            # Remove the releasing node from the queue
            new_queue = PriorityQueue()
            while  not self.queue.empty():
                q_ts, q_src, q_msg = self.queue.get()
                if q_src == msg.src:
                    continue
                new_queue.put((q_ts, q_src, q_msg))
            self.queue = new_queue

            # Sent a GRANT to the request with the highest priority
            if not self.queue.empty():
                q_ts, q_src, q_msg = self.queue.get()
                self.node.client.send_message(
                    Message(
                        Message_type.GRANT,
                        self.node.id,
                        q_src,
                        self.node.lamport_ts
                    ),
                    q_src
                )

                # Update the highest prioriry GRANT sent if needed
                hp_ts, hp_src = self.grants_sent
                if (q_ts, q_src) < (hp_ts, hp_src):
                    self.grants_sent = (q_ts, q_src)

        # Received INQUIRE
        elif msg.msg_type == Message_type.INQUIRE:

            # Reply with a YIELD if it has already failed or yielded
            if self.failed or self.yielded:
                self.node.client.send_message(
                    Message(
                        Message_type.YIELD,
                        self.node.id,
                        msg.src,
                        self.node.lamport_ts
                    ),
                    msg.src
                )
                self.yielded = True
                self.grants_received.clear()

        # Received a GRANT
        elif msg.msg_type == Message_type.GRANT:
            self.grants_received.add(msg.src)
            self.yielded = False
            self.failed = False

        # Received a FAILED
        elif msg.msg_type == Message_type.FAILED:
            self.failed = True
            self.grants_received.clear()

        # Received a message with a non valid type
        else:
            raise ValueError(f"[ValueError]: Unknown message type: {msg.msg_type}")

 