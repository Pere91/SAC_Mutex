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

        self.queue = PriorityQueue()
        self.grants_sent = None  # Track current grant as (ts, src)
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
            read_sockets, _, error_sockets = select.select(self.connection_list, [], self.connection_list, 5)
            if not read_sockets:
                print(f'NS{self.node.id} - Timed out')
                continue

            for read_socket in read_sockets:
                if read_socket == self.server_socket:
                    conn, _ = read_socket.accept()
                    self.connection_list.append(conn)
                else:
                    try:
                        msg_stream, _ = read_socket.recvfrom(4096)
                        msgs = Message.parse(msg_stream.decode("utf-8"))
                        for m in msgs:
                            self.process_message(Message.from_json(json.loads(m)))
                    except Exception as e:
                        print("Exception:", e)
                        read_socket.close()
                        self.connection_list.remove(read_socket)

        self.server_socket.close()

    def process_message(self, msg):
        print(f"Node_{self.node.id} receive msg: {msg}")

        if msg.msg_type == Message_type.REQUEST:
            self.queue.put((msg.ts, msg.src, msg))

            if self.grants_sent is None:
                # No active grant → send immediately
                ts, src, _ = self.queue.get()
                self.send_grant(src, ts)

            else:
                # Compare priorities between current grant and new request
                curr_ts, curr_src = self.grants_sent
                if (msg.ts, msg.src) < (curr_ts, curr_src):
                    # New request has higher priority → inquire current holder
                    self.node.client.send_message(
                        Message(Message_type.INQUIRE, self.node.id, curr_src, self.node.lamport_ts),
                        curr_src
                    )
                else:
                    # Lower priority → tell requester it failed for now
                    self.node.client.send_message(
                        Message(Message_type.FAILED, self.node.id, msg.src, self.node.lamport_ts),
                        msg.src
                    )

        elif msg.msg_type == Message_type.YIELD:
            # Current holder yielded → grant next in queue
            if not self.queue.empty():
                ts, src, _ = self.queue.get()
                self.send_grant(src, ts)
            self.yielded = True

        elif msg.msg_type == Message_type.RELEASE:
            # Remove the releasing node from queue if it’s still there
            new_q = PriorityQueue()
            while not self.queue.empty():
                ts, src, m = self.queue.get()
                if src != msg.src:
                    new_q.put((ts, src, m))
            self.queue = new_q

            # If there are waiting requests, grant to next
            if not self.queue.empty():
                ts, src, _ = self.queue.get()
                self.send_grant(src, ts)

            # Clear grant if released node was current
            if self.grants_sent and self.grants_sent[1] == msg.src:
                self.grants_sent = None

        elif msg.msg_type == Message_type.INQUIRE:
            if self.failed or self.yielded:
                self.node.client.send_message(
                    Message(Message_type.YIELD, self.node.id, msg.src, self.node.lamport_ts),
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

    # Helper to send grant and update tracking
    def send_grant(self, dst, ts):
        self.node.client.send_message(
            Message(Message_type.GRANT, self.node.id, dst, self.node.lamport_ts),
            dst
        )
        self.grants_sent = (ts, dst)
