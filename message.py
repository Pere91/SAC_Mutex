import json

from enum import Enum # Added
 
class Message_type(Enum):
    FAILED = 0
    YIELD = 1
    INQUIRE = 2
    REQUEST = 3
    GRANT = 4
    RELEASE = 5

class Message(object):
    def __init__(self,
            msg_type=None,
            src=None,
            dest=None,
            ts=None,
            data=None,
            ):
        self.msg_type = msg_type
        self.src = src
        self.dest = dest
        self.ts = ts
        self.data = data

    def __json__(self):
        return dict(msg_type=self.msg_type, 
            src=self.src,
            dest=self.dest, 
            ts=self.ts, 
            data=self.data)
    
    def __str__(self):
        return f"Message({self.msg_type}, {self.src}, {self.dest}, {self.ts}, {self.data})"

    def set_type(self, msg_type):
        self.msg_type = msg_type

    def set_src(self, src):
        self.src = src

    def set_dest(self, dest):
        self.dest = dest

    def set_ts(self, ts):
        self.ts = ts

    def set_data(self, data):
        self.data = data

    def to_json(self):
        obj_dict = dict()
        obj_dict['msg_type'] = self.msg_type.value
        obj_dict['src'] = self.src
        obj_dict['dest'] = self.dest
        obj_dict['ts'] = self.ts
        obj_dict['data'] = self.data
        return json.dumps(obj_dict)

    @staticmethod
    def from_json(msg):
        return Message(
            msg_type=Message_type(msg['msg_type']),
            src=msg['src'],
            dest=msg['dest'],
            ts=msg['ts'],
            data=msg['data']
        )
    
    @staticmethod
    def parse(str):
        msgs = []

        while True:
            split = str.find("}{")

            if split == -1:
                if str[-1] != '}':
                    raise ValueError("JSON must end in }")
                msgs.append(str)
                break
            
            if str[:split + 1][-1] != '}':
                raise ValueError("JSON must end in }")

            msgs.append(str[:split + 1])
            str = str[split + 1:]

        return msgs

