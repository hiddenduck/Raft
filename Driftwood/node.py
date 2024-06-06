#!/usr/bin/env python

"""Basic support to implement a Maelstrom node.

Allows initializing node (providing a default init handler), storing node_id
and node_ids, sending or replying to messages, writing message handlers or
using explicit receive (or some combination).

Messages can be provided as a dictionary or SimpleNamespace and are returned
as SimpleNamespace to allow dot notation. The message body in send and reply
functions can be defined by a dict/SimpleNamespace or/and keyword arguments.
"""

import logging
import json
import sys
from types import SimpleNamespace as sn
from threading import Lock

logging.getLogger().setLevel(logging.DEBUG)

class Node():
    def __init__(self):
        self.active_class = self
        self._node_id = None
        self._node_ids = []
        self._msg_id = 0
        self.lock = Lock()

    def node_id(self):
        """Returns node id"""
        return self._node_id

    def node_ids(self):
        """Returns list of ids of nodes in the cluster"""
        return self._node_ids
    
    def set_node_ids(self, node_ids):
        """sets list of ids of nodes in the cluster"""
        self._node_ids = node_ids

    def log(self, data, end='\n'):
        """Writes to stderr."""
        print(data, file=sys.stderr, flush=True, end=end)

    def send(self, dest, body={}, /, **kwds):
        """Sends message to dest."""
        self._msg_id += 1
        if isinstance(body, dict):
            body = body.copy()
        else:
            body = dict(vars(body))
        body.update(kwds, msg_id=self._msg_id)
        msg = dict(src=self._node_id, dest=dest, body=body)
        data = json.dumps(msg, default=vars)
        self.log("Sent " + data)
        print(data, flush=True)

    def reply(self, req, body={}, /, **kwds):
        """Sends reply message to given request."""
        self.send(req.src, body, in_reply_to=req.body.msg_id, **kwds)

    def on(self, type, handler):
        """Register handler for message type."""
        self._handlers[type] = handler

    def init(self, msg):
        # in order to avoid circular imports, we need to import inside the function
        from Follower import Follower
        from Leader import Leader
        from SharedState import SharedState
        """Default handler for init message."""
        self._node_id = msg.body.node_id
        self._node_ids = [id for id in msg.body.node_ids if id != self._node_id]

        logging.info('node %s initialized', self._node_id)

        if self.node_id() != 'n0':
            self.setActiveClass(Follower(SharedState(node=self)))

            logging.info('Follower Created')
        else:
            self.setActiveClass(Leader(SharedState(node=self)))

            logging.info('Leader Created')
        
        self.reply(msg, type='init_ok')

    def setActiveClass(self, new):
        self.active_class = new

    def getActiveClass(self):
        return self.active_class

    def _receive(self):
        data = sys.stdin.readline()
        if data:
            self.log("Received " + data, end='')
            return json.loads(data, object_hook=lambda x: sn(**x))
        else:
            return None

    def receive(self):
        """Returns next message received with no handler defined.

        Messages with handlers defined are handled and not returned.
        Returns None on EOF.
        """
        while True:
            msg = self._receive()
            if msg is None:
                return None
            else:
                lock = self.active_class.lock
                with lock:
                    if (fun := getattr(self.active_class, msg.body.type, None)) != None:
                        fun(msg)
            #Não devolver a mensagem de modo a não crashar nenhum nodo
            #else:
            #    return msg
        
if __name__ == "__main__":
    node = Node()
    node.receive()

{"id": 2,"src": "c2", "dest": "n0","body": { "type": "init","node_id": "n0","node_ids": ["n0", "n1", "n2"],"msg_id": 1 }}

{
  "id": 2,
  "src": "c2",
  "dest": "n0",
  "body": {
    "type": "read",
    "key": "string",
    "msg_id": 2
  }
}