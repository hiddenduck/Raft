from node import *

class SharedState:
    def __init__(self):
        # Lin-kv Store
        self.kv_store = dict()

        # Persistent state
        self.currentTerm = 0
        self.votedFor = None
        self.log = []

        # Volatile state
        self.commitIndex = -1
        self.lastApplied = -1