import select
from threading import Thread
import utils
from message import Message, Message_type
import json

from queue import PriorityQueue

class NodeServer(Thread):
    def __init__(self, node):
        Thread.__init__(self)
        self.node = node
        self.daemon = True

        self.queue = PriorityQueue() # Added
        self.grants_sent = None
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
                            try:
                                msgs = Message.parse(str(msg_stream, "utf-8"))
                                for m in msgs:
                                    self.process_message(Message.from_json(json.loads(m)))
                            except Exception as e: # Added
                                print("Exception: ", end="") # Added
                                print(e)
                        except:
                            read_socket.close()
                            self.connection_list.remove(read_socket)
                            continue
        
        self.server_socket.close()

    def process_message(self, msg):
        print("Node_%i receive msg: %s"%(self.node.id,msg))

        if msg.msg_type == Message_type.REQUEST:

            if self.grants_sent:
                hp_ts, hp_src = self.grants_sent

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
                    
        elif msg.msg_type == Message_type.YIELD:
            q_ts, q_src, q_msg = self.queue.get()
            self.queue.put((q_ts, q_src, q_msg))
            self.node.client.send_message(
                Message(
                    Message_type.GRANT,
                    self.node.id,
                    q_src,
                    self.node.lamport_ts
                ),
                q_src
            )
            self.grants_sent = (q_ts, q_src)
            self.queue.put((msg.ts, msg.src, msg))
            if msg.src in self.grants_sent:
                self.grants_sent.remove(msg.src)

        elif msg.msg_type == Message_type.RELEASE:
            new_queue = PriorityQueue()
            while  not self.queue.empty():
                q_ts, q_src, q_msg = self.queue.get()
                if q_src == msg.src:
                    continue
                new_queue.put((q_ts, q_src, q_msg))
            
            self.queue = new_queue

            # Sent a grant message to the request with the highest priority
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
                self.grants_sent = (q_ts, q_src)

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
                self.grants_received.clear()

        elif msg.msg_type == Message_type.GRANT:
            self.grants_received.add(msg.src)
            self.yielded = False
            self.failed = False

        elif msg.msg_type == Message_type.FAILED:
            self.failed = True
            self.grants_received.clear()

        else:
            raise ValueError(f"[ValueError]: Unknown message type: {msg.msg_type}")

 