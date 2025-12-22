import select
from threading import Thread
import utils
from message import Message, Message_type
import json
import logger_config

LOG_FILE_PATH = "logs/log.log"

flog = logger_config.get_file_logger(LOG_FILE_PATH, logger_config.logging.DEBUG)
clog = logger_config.get_console_logger(logger_config.logging.INFO)

class NodeServer(Thread):
    """
    Handles a node's receiving message operations and the responses to them.

    Attributes:
        node (Node): Node that receives the messages as a server.
        daemon (bool): Thread's daemon option.
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
                self.connection_list, [], [], 20)
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
        Determines which type of message is received and calls the
        corresponding handler.

        Args:
            msg (Message): Message received.

        Raises:
            ValueError: If the type of the Message is not valid.
        """
        clog.info("Node_%i receive msg: %s"%(self.node.id,msg))
        flog.info("Node_%i receive msg: %s"%(self.node.id,msg))

        # Update Lamport timestamp
        self.node.lamport_ts = max(self.node.lamport_ts, msg.ts) + 1

        # Received a REQUEST
        if msg.msg_type == Message_type.REQUEST:
            self.node.request_handler(msg)

        # Received a YIELD            
        elif msg.msg_type == Message_type.YIELD:
            self.node.yield_handler(msg)

        # Received a RELEASE
        elif msg.msg_type == Message_type.RELEASE:
            self.node.release_handler(msg)

        # Received INQUIRE
        elif msg.msg_type == Message_type.INQUIRE:
            self.node.inquire_handler(msg)

        # Received a GRANT
        elif msg.msg_type == Message_type.GRANT:
            self.node.grant_handler(msg)

        # Received a FAILED
        elif msg.msg_type == Message_type.FAILED:
            self.node.failed_handler(msg)

        # Received a message with a non valid type
        else:
            raise ValueError(f"[ValueError]: Unknown message type: {msg.msg_type}")