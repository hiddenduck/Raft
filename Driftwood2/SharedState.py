from node import *
from threading import Lock
from bitarray import bitarray
# import Node_Timer
from Node_Timer import Node_Timer
import random

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
        self.roundLC     =  0

        # Other
        self.timer = Node_Timer(0.150, 0.300)

        self.node = node

        # New Data Structures
        self.bitarray = (len(self.node.node_ids())+1) * bitarray('0')

        self.nextCommit = -1
        self.maxCommit  = -1

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

    def checkCommitIndex(self): #isto pode mudar se: log/maxCommit mudar
        if self.currentTerm == self.log[-1][1]:
            self.commitIndex = min(self.maxCommit, len(self.log)-1)
            if self.commitIndex > self.lastApplied:
                self.applyLogEntries(self.log[self.lastApplied:self.commitIndex+1])
                self.lastApplied = self.commitIndex

    def checkBitmap(self): #isto pode mudar se: log/nextCommit mudar, true se mudou o bitmap
        if  self.nextCommit < len(self.log) and \
            self.log[self.nextCommit][1] == self.currentTerm:
            self.bitarray.set(int(self.node.node_id()[1:]))

    def newTerm(self, newTerm, votedFor=None):
        self.roundLC = 0
        self.currentTerm = newTerm
        self.bitarray.setall(0)
        self.votedFor = votedFor
        self.nextCommit = self.maxCommit + 1
        self.checkBitmap()

    def updateBitmap(self): #muda possivelmente se bitmap mudar
        if self.bitarray.count() > (self.bitarray.size() / 2.0):
            self.maxCommit = self.nextCommit
            self.checkCommitIndex()
            self.bitarray.setall(0)
            if self.nextCommit >= len(self.log)-1 or \
               self.currentTerm != self.log[-1][1]:
                self.nextCommit = self.nextCommit+1
            else:
                self.nextCommit = len(self.log)-1
                self.bitarray.set(int(self.node.node_id()[1:]))

    def mergeBitmap(self, bitarray, maxCommit, nextCommit):
        if maxCommit > self.maxCommit:
            self.maxCommit = maxCommit #sempre que o maxcommit muda testa-se o commit index
            self.checkCommitIndex()
        if self.nextCommit <= self.maxCommit:
            self.bitarray = bitarray
            self.nextCommit = nextCommit
            self.checkBitmap()
            self.updateBitmap()
        elif self.nextCommit <= nextCommit:
            self.bitarray |= bitarray
            self.updateBitmap()
    
    def sendEntries(self, leaderId, isRPC=False):
        self.roundLC += 1
        ids = self.node.node_ids()
        len_ids = len(ids)

        for i in range(self.funout):
            dest_id = ids[(self.roundLC + i) % len_ids]
            self.node.send(dest_id, type="appendEntries", message=(
                    self.currentTerm, # term
                    leaderId, #leaderId
                    self.commitIndex, # prevLogIndex
                    self.log[self.commitIndex][1] if self.commitIndex >= 0 else -1, # prevLogTerm
                    [self.log[i] for i in range(self.commitIndex+1,len(self.log))], # entries[]
                    self.roundLC, #leaderRound
                    isRPC, #isRPC
                    self.bitarray, #bitmap
                    self.maxCommit, #maxCommit
                    self.nextCommit #nextCommit
                    ) 
            )
        
        self.roundLC += self.funout

    def create_peer_permutation(self):
        ids = self.node.node_ids()
        self.node.set_node_ids([peer for peer in random.sample(ids, len(ids))])