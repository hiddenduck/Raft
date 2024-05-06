from node import *
# import Node_Timer
from Node_Timer import Node_Timer

class SharedState:
    def __init__(self):
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

    def getState(self):
        return self
    
    def changeState(self, sharedState):
        # Lin-kv Store
        self.kv_store = sharedState.kv_store

        # Persistent state
        self.currentTerm = sharedState.currentTerm
        self.votedFor = None
        self.log = sharedState.currentTerm

        # Volatile state
        self.commitIndex = sharedState.commitIndex
        self.lastApplied = sharedState.lastApplied
