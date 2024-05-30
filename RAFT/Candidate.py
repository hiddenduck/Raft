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

        self.timer.create(lambda: self.node.send(self.node.node_id(), type="resetElection"))
        self.timer.start()

    def startRequestVote(self):
        lenLog = len(self.log)
        for dest_id in self.node.node_ids():
            self.node.send(dest_id, type="requestVote", term=self.currentTerm, lastLogIndex = lenLog, lastLogTerm = self.log[-1][1] if lenLog!=0 else 0)
    
    def requestVote(self, msg):
        term = msg.body.term

        if term > self.currentTerm:
            self.currentTerm = term

            _, lastLogTerm = self.log[-1]
            if  msg.body.lastLogTerm < lastLogTerm or \
                (msg.body.lastLogTerm == lastLogTerm and msg.body.lastLogIndex < len(self.log)):
                self.votedFor = None
                #Não devíamos responder se não vai mudar em nada
                self.node.reply(msg, type='handleVote', term=term, voteGranted=False) #todo: reply false, is it worth tho? in the paper says to reply false
            else:
                self.votedFor = msg.src
                self.node.reply(msg, type='handleVote', term=term, voteGranted=True)

            self.becomeFollower()
        else:
            #Ligeiramente diferente dos votos dos líderes, só avisa se o seu termo for maior
            self.node.reply(msg, type="handleVote", term=self.currentTerm, voteGranted = False)

    def handleVote(self, msg):
        if msg.body.voteGranted:
            self.voters.add(msg.src)

            if len(self.voters) >= len(self.node.node_ids()) / 2: # case (a): a Candidate received majority of votes
                self.node.setActiveClass(Leader())

        elif self.currentTerm < msg.body.term:
            self.currentTerm = msg.body.term
            self.becomeFollower()

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        success = False
        changeType = False

        if term >= self.currentTerm: # if a valid leader contacts:
            self.currentTerm = term
            changeType = True

            if len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex][1] == prevLogTerm):
                success = True
                
                if prevLogIndex >= 0:
                    self.log = self.log[:prevLogIndex+1] + entries
            
                if leaderCommit > self.commitIndex:
                    self.commitIndex = min(leaderCommit, len(self.log)-1)
                    self.applyLogEntries(self.log[self.lastApplied:self.commitIndex+1])
                    self.lastApplied = self.commitIndex
                    
        if success:
            self.node.reply(msg, type="appendEntries_success", term=self.currentTerm)
        else:
            self.node.reply(msg, type="appendEntries_insuccess", term=self.currentTerm)

        if changeType:
            self.becomeFollower()

    #Estas funções têm de ser sempre as últimas a serem invocadas num método (garantir que houve troca)
    def becomeFollower(self):
        from Follower import Follower
        self.node.setActiveClass(Follower(super().getState()))

    def resetElection(self, msg):
        self.node.setActiveClass(Candidate(super().getState()))
