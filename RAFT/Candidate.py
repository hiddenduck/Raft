from SharedState import SharedState
from Leader import Leader
from node import *

class Candidate(SharedState):
    def __init__(self, sharedState):
        super().__init__()
        # Set State
        super().changeState(sharedState)
        # Candidate 
        self.voters = {self.node.node_id()}
        self.votedFor = self.node.node_id()
        self.currentTerm += 1
        self.startRequestVote()

        self.timer.create(lambda s: s.resetElection(), self)
        self.timer.start()

    def resetElection(self):
        self.node.log('Reseted Election')
        lock = self.lock
        with lock:
            if self.node.active_class == self:
                self.node.setActiveClass(Candidate(super().getState()))

    def startRequestVote(self):
        lenLog = len(self.log)
        for dest_id in self.node.node_ids():
            self.node.send(dest_id, type="requestVote", term=self.currentTerm, lastLogIndex = lenLog-1, lastLogTerm = self.log[-1][1] if lenLog!=0 else 0)
    
    def requestVote(self, msg):
        term = msg.body.term

        if term > self.currentTerm:
            self.timer.stop()
            self.currentTerm = term

            if  len(self.log) > 0 and \
                (msg.body.lastLogTerm < self.log[-1][1] or \
                (msg.body.lastLogTerm == self.log[-1][1] and msg.body.lastLogIndex < len(self.log)-1)):
                #Não devíamos responder se não vai mudar em nada
                self.node.reply(msg, type='handleVote', term=term, voteGranted=False) #todo: reply false, is it worth tho? in the paper says to reply false
                self.becomeFollower()
            else:
                self.node.reply(msg, type='handleVote', term=term, voteGranted=True)
                self.becomeFollower(msg.src)

        else:
            #Ligeiramente diferente dos votos dos líderes, só avisa se o seu termo for maior
            self.node.reply(msg, type="handleVote", term=self.currentTerm, voteGranted = False)

    def handleVote(self, msg):
        if msg.body.voteGranted:
            self.voters.add(msg.src)

            if len(self.voters) > (len(self.node.node_ids())+1) / 2.0: # case (a): a Candidate received majority of votes
                self.timer.stop()
                self.becomeLeader()

        elif self.currentTerm < msg.body.term:
            self.timer.stop()
            self.currentTerm = msg.body.term
            self.becomeFollower()

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        success = False
        changeType = False

        if term >= self.currentTerm: # if a valid leader contacts:
            self.timer.stop()
            changeType = True
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
                    self.applyLogEntries(self.log[self.lastApplied+1:self.commitIndex+1])
                    self.lastApplied = self.commitIndex

                if entries or prevLogIndex != len(self.log)-1:
                    self.node.reply(msg, type="appendEntries_success", term=self.currentTerm, nextIndex=len(self.log))
                    
        if not success:
            self.node.reply(msg, type="appendEntries_insuccess", term=self.currentTerm, nextIndex=len(self.log))

        if changeType:
            self.becomeFollower()

    #Estas funções têm de ser sempre as últimas a serem invocadas num método (garantir que houve troca)
    def becomeFollower(self, votedFor=None):
        from Follower import Follower
        self.votedFor=votedFor
        self.node.setActiveClass(Follower(super().getState()))

    def becomeLeader(self):
        from Leader import Leader
        self.node.setActiveClass(Leader(super().getState()))
