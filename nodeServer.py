import select
from threading import Thread
import utils
from message import Message, Message_type
import json

from queue import PriorityQueue # Added 

class NodeServer(Thread):
    def __init__(self, node):
        Thread.__init__(self)
        self.node = node
        self.daemon = True

        self.queue = PriorityQueue() # Added
        self.grants_sent = set()
        self.grants_received = set()
        self.yielded = False
        self.failed = False
    
    def run(self):
        self.update()

    def update(self):
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
                            #for msg in msg_stream:
                            try:
                                ms = json.loads(str(msg_stream,"utf-8"))
                                m = Message.from_json(ms)
                                self.process_message(m)
                            except Exception as e: # Added
                                print(e) # Added
                        except:
                            read_socket.close()
                            self.connection_list.remove(read_socket)
                            continue
        
        self.server_socket.close()

    def process_message(self, msg):
        #TODO MANDATORY manage the messages according to the Maekawa algorithm (TIP: HERE OR IN ANOTHER FILE...)
        print("Node_%i receive msg: %s"%(self.node.id,msg))

        if msg.msg_type == Message_type.REQUEST:
            if self.grants_sent:
                hp_grant = sorted(self.grants_sent)[0]

                if hp_grant < msg.src:
                    self.node.client.send_message(
                        Message(
                            Message_type.FAILED,
                            self.node.id,
                            msg.src,
                            self.node.lamport_ts
                        ),
                        msg.src
                    )
                    self.queue.put((msg.src, msg))

                else:
                    self.node.client.send_message(
                        Message(
                            Message_type.INQUIRE,
                            self.node.id,
                            hp_grant,
                            self.node.lamport_ts
                        ),
                        hp_grant
                    )

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
                self.grants_sent.add(msg.src)
                    
        elif msg.msg_type == Message_type.YIELD:
            q_src, q_msg = self.queue.get()
            self.queue.put((q_src, q_msg))
            self.node.client.send_message(
                Message(
                    Message_type.GRANT,
                    self.node.id,
                    q_src,
                    self.node.lamport_ts
                ),
                q_src
            )
            self.grants_sent.add(q_src)
            self.queue.put((msg.src, msg))
            self.grants_sent.remove(msg.src)

        elif msg.msg_type == Message_type.RELEASE:
            msgs = []

            # Delete the message source from the request queue
            q_src, q_msg = self.queue.get()
            while q_src != msg.src:
                msgs.append((q_src, q_msg))
                q_src, q_msg = self.queue.get()
            self.grants_sent.remove(msg.src)
            
            # Put all other extracted messages back into the queue
            for m in msgs:
                self.queue.put(m)

            # Sent a grant message to the request with the highest priority
            q_src, q_msg = self.queue.get()
            self.node.client.send_message(
                Message(
                    Message_type.GRANT,
                    self.node.id,
                    q_src,
                    self.node.lamport_ts
                ),
                q_src
            )
            self.grants_sent.add(q_src)

        elif msg.msg_type == Message_type.INQUIRE:
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

        elif msg.msg_type == Message_type.GRANT:
            self.grants_received.add(msg.src)
            self.yielded = False
            self.failed = False

        elif msg.msg_type == Message_type.FAILED:
            self.failed = True

        else:
            raise ValueError(f"[ValueError]: Unknown message type: {msg.msg_type}")

 