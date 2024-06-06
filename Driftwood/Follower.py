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
            self.newTerm(term, votedFor=msg.src)
            self.node.reply(msg, type='handleVote', term=term, voteGranted=True)
            self.timer.reset()

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit, leaderRound, isRPC, bitmap, maxCommit, nextCommit = tuple(msg.body.message)
        success = False

        if term > self.currentTerm:
            self.newTerm(term, votedFor=leaderID)

        if term >= self.currentTerm:
            self.timer.stop()
            self.merge(bitmap, maxCommit, nextCommit)
            if (isRPC or leaderRound > self.roundLC):
                if len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex][1] == prevLogTerm):
                    success = True
                    if prevLogIndex >= 0:
                        self.log = self.log[:prevLogIndex+1] + entries
                    else:
                        self.log = entries
                    
                else:
                    self.log = entries[:prevLogIndex]

                #sempre que o log muda testa-se o commitindex
                self.updateCommitIndex()
            self.timer.reset()

        if not success:
            self.node.send(leaderID, type="appendEntries_insuccess", term=self.currentTerm, lastLogIndex=min(len(self.log)-1, prevLogIndex-1))
        elif not isRPC:
            self.roundLC = leaderRound
            #TODO Gossip request
            undefined
        else:
            self.node.send(leaderID, type="appendEntries_success", term=self.currentTerm, lastLogIndex=len(self.log)-1)
                