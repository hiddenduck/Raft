from node import *
from threading import Lock
# import Node_Timer
from Node_Timer import Node_Timer
import random
from math import log as math_log

class SharedState:
    def __init__(self, node=None, votedFor=None):
        # Lin-kv Store
        self.kv_store = dict()
        self.kv_log_store = dict()
        
        # Persistent state
        self.currentTerm = 0
        self.votedFor = votedFor
        self.log = [] # log format -> ((key,value), term)

        # Volatile state
        self.commitIndex = -1
        self.lastApplied = -1
        self.roundLC     =  0

        # Other
        self.timer = Node_Timer(0.150, 0.300)

        # Gossip
        if node != None:
            self.node = node
            self.fanout = int(math_log(len(self.node.node_ids()))) + 1 # fanout + C

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
        self.roundLC = sharedState.roundLC

        self.fanout = sharedState.fanout
    
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

    def sendEntries(self, leaderId, leaderCommit, isRPC=False):
        ids = self.node.node_ids()
        len_ids = len(ids)

        for i in range(self.fanout):
            dest_id = ids[(self.roundLC + i) % len_ids]
            self.node.send(dest_id, type="appendEntries", message=(
                    self.currentTerm, # term
                    leaderId, #leaderId
                    self.commitIndex, # prevLogIndex
                    self.log[self.commitIndex][1] if self.commitIndex >= 0 else -1, # prevLogTerm
                    [self.log[i] for i in range(self.commitIndex+1,len(self.log))], # entries[]
                    leaderCommit, # leaderCommit
                    self.roundLC, #leaderRound
                    isRPC #isRPC
                    ) 
            )

    def create_peer_permutation(self):
        ids = self.node.node_ids()
        self.node.set_node_ids([peer for peer in random.sample(ids, len(ids))])
    