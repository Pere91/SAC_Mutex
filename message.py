import json
from enum import Enum
 
class Message_type(Enum):
    """
    All the message types available.
    """
    FAILED = 0
    YIELD = 1
    INQUIRE = 2
    REQUEST = 3
    GRANT = 4
    RELEASE = 5

class Message(object):
    """
    Represents a message sent between nodes.

    Attributes:
        msg_type (Message_Type): Type of the message.
        src (int): Id of the message sender.
        dest (int): Id of the message receiver.
        ts (int): Lamport timestamp of the message sending.
        data (any, optional): Content of the message. Defaults to None.
    """
    def __init__(self,
            msg_type=None,
            src=None,
            dest=None,
            ts=None,
            data=None,
            ):
        """
        Constructor for class Message.

        Args:
            msg_type (Message_Type): Type of the message. Defaults to None.
            src (int): Id of the message sender. Defaults to None.
            dest (int): Id of the message receiver. Defaults to None.
            ts (int): Lamport timestamp of the message sending. Defaults to None.
            data (any, optional): Content of the message. Defaults to None.
        """
        self.msg_type = msg_type
        self.src = src
        self.dest = dest
        self.ts = ts
        self.data = data

    def __json__(self):
        """
        Put Message in JSON format.

        Returns:
            dict: JSON with the fields of the message.
        """
        return dict(msg_type=self.msg_type, 
            src=self.src,
            dest=self.dest, 
            ts=self.ts, 
            data=self.data)
    
    def __str__(self):
        """
        Put Message in string format.

        Returns:
            str: String displaying the fields of the message.
        """
        return f"Message({self.msg_type}, {self.src}, {self.dest}, {self.ts}, {self.data})"

    def set_type(self, msg_type):
        """
        Setter for msg_type field.

        Args:
            msg_type (Message_type): Type which to update the message to.
        """
        self.msg_type = msg_type

    def set_src(self, src):
        """
        Setter for src field.

        Args:
            src (int): Updated value for the source of the message.
        """
        self.src = src

    def set_dest(self, dest):
        """
        Setter for dest field.

        Args:
            dest (int): Updated value for the destination of the message.
        """
        self.dest = dest

    def set_ts(self, ts):
        """
        Setter for ts field.

        Args:
            ts (int): Updated value for the Lamport timestamp of the message.
        """
        self.ts = ts

    def set_data(self, data):
        """
        Setter for the data field.

        Args:
            data (any): New data content for the message.
        """
        self.data = data

    def to_json(self):
        """
        Serializes the JSON representation of the Message.

        Returns:
            str: Serialized JSON.
        """
        obj_dict = dict()
        obj_dict['msg_type'] = self.msg_type.value
        obj_dict['src'] = self.src
        obj_dict['dest'] = self.dest
        obj_dict['ts'] = self.ts
        obj_dict['data'] = self.data
        return json.dumps(obj_dict)

    @staticmethod
    def from_json(msg):
        """
        Converts a JSON back into a Message.

        Args:
            msg (dict): Jsonified Message.

        Returns:
            Message: Message with the fields in the input JSON.
        """
        return Message(
            msg_type=Message_type(msg['msg_type']),
            src=msg['src'],
            dest=msg['dest'],
            ts=msg['ts'],
            data=msg['data']
        )
    
    @staticmethod
    def parse(str):
        """
        Parses a serialized jsonified Message to identify possible joined
        back-to-back JSONs. In case of finding any, splits them in single
        serialized JSONs.

        Args:
            str (str): Serialized stream with one or more JSONs.

        Raises:
            ValueError: If some JSON does not end in }.

        Returns:
            list: All the serialized JSONs separated.
        """
        msgs = []

        while True:
            # Find if there is some back-to-back JSONs
            split = str.find("}{")

            # If there aren't and JSON format is correct, end
            if split == -1:
                if str[-1] != '}':
                    raise ValueError("JSON must end in }")
                msgs.append(str)
                break
            
            # If there are, split the JSON from the rest of the stream and
            # repeat until there aren't.
            if str[:split + 1][-1] != '}':
                raise ValueError("JSON must end in }")
            msgs.append(str[:split + 1])
            str = str[split + 1:]

        return msgs

