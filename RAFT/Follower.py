import SharedState
from node import *

class Follower(SharedState):
    def __init__(self, node_id, node_ids):
        super().__init__(node_id, node_ids)
        
    #TODO Criar thread que dá timeout para um valor aleatório entre 150-300

    def read(self, msg):
        reply(msg, type='error', code='11', text='not the leader')

    def write(self, msg):
        reply(msg, type='error', code='11', text='not the leader')

    def cas(self, msg):
        reply(msg, type='error', code='10', text='unsupported')

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        

        if term >= self.currentTerm and len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex] == prevLogTerm):
            self.currentTerm = term
            self.log = log[0:prevLogIndex+1] + entries
            
            if leaderCommit > self.commitIndex:
                self.lastApplied = self.commitIndex
                self.commitIndex = min(leaderCommit, len(self.log)-1)
                applyLogEntries(self.lastApplied)
            
            reply(msg, type="appendEntries_success", term=self.currentTerm)
        else:
            reply(msg, type="appendEntries_insuccess")
