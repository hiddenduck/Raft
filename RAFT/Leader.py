from SharedState import SharedState
from node import *

class Leader(SharedState):
    def __init__(self, sharedState):
        super().__init__()
        # set State
        super().changeState(sharedState)
        # Volatile state
        self.nextIndex = {}
        self.matchIndex = {}
        for node in node_ids():
            self.nextIndex[node] = self.currentTerm + 1
            self.matchIndex[node] = -1
            
    
    def read(self, msg):

        kv_store =self.kv_store

        if msg.body.key in kv_store:
            reply(msg, type='read_ok', value=kv_store[msg.body.key])
        else:
            reply(msg, type='error', code='20', text='key does not exist')
    
    def write(self, msg):
        self.log.append((msg, self.currentTerm))

        for dest_id in node_ids():
            if len(self.log) >= self.nextIndex[dest_id]:
                send(dest_id, type="appendEntries", message=(
                    self.currentTerm, # term
                    node_id(), #leaderId
                    self.nextIndex[dest_id]-1, # prevLogIndex
                    self.log[self.nextIndex[dest_id]-1], # prevLogTerm
                    [self.log[i] for i in range(self.nextIndex[dest_id],len(self.log))], # entries[]
                    self.commitIndex) # leaderCommit
                )
                
    def appendEntries_success(self, msg):
        self.nextIndex[msg.src] = msg.body.nextIndex
        self.matchIndex[msg.src] = msg.body.nextIndex

        # Detect Majority

        count = 0
        candidate = None

        for replica in self.matchIndex.keys():
            if count == 0:
                candidate = self.matchIndex[replica]
                count = 1
            else:
                count = count+1 if self.matchIndex[replica] == candidate else count-1
        
        count = 0

        for replica in self.matchIndex.keys():
            if self.matchIndex[replica] == candidate:
                count += 1
        
        if count > len(self.matchIndex.keys())/2 and candidate > self.commitIndex:
            for toBeCommited in log[self.commitIndex:candidate]:
                body = self.log[toBeCommited][0].body
                self.kv_store[body.key] = body.value
            self.commitIndex = candidate

    def appendEntries_insuccess(self, msg):
        dest_id = msg.src
        
        self.nextIndex[dest_id] -= 1
        send(dest_id, type="appendEntries", message=(
            self.currentTerm, # term
            node_id(), #leaderId
            self.nextIndex[dest_id]-1, # prevLogIndex
            self.log[self.nextIndex[dest_id]-1], # prevLogTerm
            [self.log[i] for i in range(self.nextIndex[dest_id],len(self.log))], # entries[]
            self.commitIndex) # leaderCommit
            )
