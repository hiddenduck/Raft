from SharedState import SharedState
from Candidate import Candidate
from node import *

class Follower(SharedState):
    def __init__(self, sharedState, leaderMsg = None):
        super().__init__()
        # Set State
        super().changeState(sharedState)
        #Volatile State

        self.timer.create(lambda s: s.startElection(), self)
        self.timer.start()

        if leaderMsg:
            self.appendEntries(leaderMsg)        
    
    def startElection(self):
        self.node.log('Started Election')
        lock = self.lock
        with lock:
            if self.node.active_class == self:
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
            self.roundLC = 0
            self.node.reply(msg, type='handleVote', term=term, voteGranted=True)
            self.timer.reset()

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit, leaderRound, isRPC = tuple(msg.body.message)

        if term > self.currentTerm:
            self.roundLC = 0
            self.votedFor = leaderID

        if term >= self.currentTerm:
            if (isRPC or leaderRound > self.roundLC):
                self.timer.stop()
                self.currentTerm = term

                if len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex][1] == prevLogTerm):
                    if prevLogIndex >= 0:
                        self.log = self.log[:prevLogIndex+1] + entries
                    else:
                        self.log = entries
                
                    if leaderCommit > self.commitIndex:
                        self.commitIndex = min(leaderCommit, len(self.log)-1)
                        self.applyLogEntries(self.log[self.lastApplied:self.commitIndex+1])
                        self.lastApplied = self.commitIndex

                    self.node.reply(msg, type="appendEntries_success", term=self.currentTerm, lastLogIndex=len(self.log))
                else:
                    self.log = entries[:prevLogIndex]
                    self.node.reply(msg, type="appendEntries_insuccess", term=self.currentTerm, lastLogIndex=min(len(self.log), prevLogIndex-1))

                if not isRPC:
                    self.roundLC = leaderRound
                    #TODO Gossip request
                    undefined

                self.timer.reset()
            elif isRPC:
                self.timer.stop()
                self.node.reply(msg, type="appendEntries_insuccess", term=self.currentTerm, lastLogIndex=min(len(self.log), prevLogIndex-1))
                self.timer.reset()

