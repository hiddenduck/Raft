from node import *
import Node_Timer

class SharedState:
    def __init__(self, node_id, node_ids):
        # Maelstrom
        self.node_id = node_id
        self.node_ids = node_ids

        # Lin-kv Store
        self.kv_store = dict()

        # Persistent state
        self.currentTerm = 0
        self.votedFor = None
        self.log = []

        # Volatile state
        self.commitIndex = 0
        self.lastApplied = 0

        # Other
        self.timer = Node_Timer(150, 300)