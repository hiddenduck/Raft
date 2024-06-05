from node import *
from threading import Lock
# import Node_Timer
from Node_Timer import Node_Timer

class SharedState:
    def __init__(self, node=None):
        # Lin-kv Store
        self.kv_store = dict()
        self.kv_log_store = dict()
        
        # Persistent state
        self.currentTerm = 0
        self.votedFor = None
        self.log = [] # log format -> ((key,value), term)

        # Volatile state
        self.commitIndex = -1
        self.lastApplied = -1

        # Other
        self.timer = Node_Timer(0.150, 0.300)

        self.node = node

        self.lock = Lock()

    def getState(self):
        return self
    
    def changeState(self, sharedState):
        # Lin-kv Store
        self.kv_store = sharedState.kv_store

        # Persistent state
        self.currentTerm = sharedState.currentTerm
        self.votedFor = sharedState.votedFor
        self.log = sharedState.log

        # Volatile state
        self.commitIndex = sharedState.commitIndex
        self.lastApplied = sharedState.lastApplied

        self.node = sharedState.node
    
    def read(self, msg):
        if self.votedFor != None and self.votedFor != self.node.node_id():
            self.node.send(self.votedFor, type='read_redirect', msg=msg)
        else:
            self.node.reply(msg, type='error', code='11', text='not the leader')

    def write(self, msg):
        if self.votedFor != None and self.votedFor != self.node.node_id():
            self.node.send(self.votedFor, type='write_redirect', msg=msg)
        else:
            self.node.reply(msg, type='error', code='11', text='not the leader')

    def cas(self, msg):
        if self.votedFor != None and self.votedFor != self.node.node_id():
            self.node.send(self.votedFor, type='cas_redirect', msg=msg)
        else:
            self.node.reply(msg, type='error', code='11', text='not the leader')

    def applyLogEntries(self, entries):
        for (msg, _) in entries:
            self.kv_store[msg.body.key] = msg.body.value
    
    def updateCommitIndex(self):
        self.commitIndex = min(self.maxCommit, len(self.log)-1)
        if self.commitIndex > self.lastApplied:
            self.applyLogEntries(self.log[self.lastApplied:self.commitIndex+1])
            self.lastApplied = self.commitIndex