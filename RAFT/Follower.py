from SharedState import SharedState
from Candidate import Candidate
from node import *

class Follower(SharedState):
    def __init__(self, sharedState, leaderMsg = None):
        super().__init__()
        # Set State
        super().changeState(sharedState)

        self.timer.create(lambda node: node.send(node.node_id(), type="startElection"), self.node)
        self.timer.start()

        if leaderMsg:
            self.appendEntries(leaderMsg)        
    
    def startElection(self, msg):
        self.timer.stop()
        self.node.setActiveClass(Candidate(super().getState()))

    def requestVote(self, msg):
        term = msg.body.term

        if  term < self.currentTerm or \
            (term == self.currentTerm and self.votedFor != None and self.votedFor != msg.src) or \
            (len(self.log) > 0 and \
            (msg.body.lastLogTerm < self.log[-1][1] or \
            (msg.body.lastLogTerm == self.log[-1][1] and msg.body.lastLogIndex < len(self.log)))):
            self.node.reply(msg, type='handleVote', term=self.currentTerm, voteGranted=False) #todo: reply false, is it worth tho? in the paper says to reply false
        else:
            self.timer.stop()
            self.votedFor = msg.src
            self.currentTerm = term
            self.node.reply(msg, type='handleVote', term=term, voteGranted=True)
            self.timer.reset()

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        success = False

        if term >= self.currentTerm:
            self.timer.stop()
            self.currentTerm = term
            self.votedFor = msg.src

            if len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex][1] == prevLogTerm):
                success = True

                if prevLogIndex >= 0:
                    self.log = self.log[:prevLogIndex+1] + entries
                else:
                    self.log = entries
            
                if leaderCommit > self.commitIndex:
                    self.commitIndex = min(leaderCommit, len(self.log)-1)
                    self.applyLogEntries(self.log[self.lastApplied:self.commitIndex+1])
                    self.lastApplied = self.commitIndex

            self.timer.reset()

        if success:
            self.node.reply(msg, type="appendEntries_success", term=self.currentTerm, nextIndex=len(self.log))
        else:
            self.node.reply(msg, type="appendEntries_insuccess", term=self.currentTerm, nextIndex=len(self.log))

