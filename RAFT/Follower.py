from SharedState import SharedState
from Candidate import Candidate
from node import *

class Follower(SharedState):
    def __init__(self, sharedState):
        super().__init__()
        # Set State
        super().changeState(sharedState)
        
        self.timer.create(lambda: send(node_id(), type="startElection"))
        self.timer.start()

    def __init__(self, sharedState, leaderMsg):
        self.__init__(sharedState)

        self.appendEntries(leaderMsg)
    
    # Maelstrom
    def read(self, msg):
        reply(msg, type='error', code='11', text='not the leader')

    def write(self, msg):
        reply(msg, type='error', code='11', text='not the leader')

    def cas(self, msg):
        reply(msg, type='error', code='11', text='not the leader')

    def startElection(self, msg):
        setActiveClass(Candidate(super().getState()))


    def applyLogEntries(self, entries):
        for ((key, value), _) in entries:
            self.kv_store[key] = value

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        if term >= self.currentTerm and len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex] == prevLogTerm):
            self.currentTerm = term
            self.log = log[:prevLogIndex+1] + entries
            
            if leaderCommit > self.commitIndex:
                self.commitIndex = min(leaderCommit, len(self.log)-1)
                self.applyLogEntries(self.log[self.lastApplied:self.commitIndex+1])
                self.lastApplied = self.commitIndex

            reply(msg, type="appendEntries_success", term=self.currentTerm)
        else:
            reply(msg, type="appendEntries_insuccess")
