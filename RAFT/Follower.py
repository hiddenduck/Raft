from node import *

class Follower:
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
    
    def read(self, msg):
        reply(msg, type='error', code='11', text='not the leader')

    def write(self, msg):
        reply(msg, type='error', code='11', text='not the leader')

    def cas(self, msg):
        reply(msg, type='error', code='10', text='unsupported')

    def appendEntries(self, msg):
        leaderID = msg.src
        leaderTerm, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        if leaderID == node_ids()[0]:
            if leaderTerm >= self.currentTerm and len(self.log) > prevLogIndex and self.log[prevLogIndex] == prevLogTerm:
                for i,entry in enumerate(entries):
                    self.log[i+prevLogIndex] = entry
                
                if leaderCommit > self.commitIndex:
                    self.commitIndex = min(leaderCommit, len(self.log))
                
                reply(msg, type="appendEntries_success", nextIndex=len(self.log))
            else:
                reply(msg, type="appendEntries_insuccess")
