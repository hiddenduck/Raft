from SharedState import SharedState
from Leader import Leader
from node import *

class Candidate(SharedState):
    def __init__(self, sharedState):
        super().__init__()
        # Set State
        super().changeState(sharedState)
        # Candidate 
        self.voters = {node_id()}
        self.votedFor = node_id()
        self.currentTerm += 1
        self.startRequestVote()

        self.timer.create(lambda: send(node_id(), type="resetElection"))
        self.timer.start()

    def startRequestVote(self):
        lenLog = len(self.log)
        for dest_id in node_ids():
            send(dest_id, type="requestVote", term=self.currentTerm, lastLogIndex = lenLog, lastLogTerm = self.log[-1][1] if lenLog!=0 else 0)
    
    def requestVote(self, msg):
        term = msg.body.term

        if term > self.currentTerm:
            self.currentTerm = term

            _, lastLogTerm = self.log[-1]
            if  msg.body.lastLogTerm < lastLogTerm or \
                (msg.body.lastLogTerm == lastLogTerm and msg.body.lastLogIndex < len(self.log)):
                self.votedFor = None
                reply(msg, type='handleVote', term=term, voteGranted=False) #todo: reply false, is it worth tho? in the paper says to reply false
            else:
                self.votedFor = msg.src
                reply(msg, type='handleVote', term=term, voteGranted=True)

            self.becomeFollower()
        else:
            reply(msg, type="handleVote", term=self.currentTerm, voteGranted = False)

    def handleVote(self, msg):
        if msg.body.voteGranted:
            self.voters.add(msg.src)

            if len(self.voters) >= len(node_ids()) / 2: # case (a): a Candidate received majority of votes
                setActiveClass(Leader())

        elif msg.body.term >= self.currentTerm:
            self.currentTerm = msg.body.term
            self.becomeFollower()

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        if term >= self.currentTerm: # if a valid leader contacts:
            if len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex][1] == prevLogTerm):
                self.currentTerm = term
                self.log = log[:prevLogIndex+1] + entries
                
                if prevLogIndex >= 0:
                    self.log = self.log[:prevLogIndex+1] + entries

                if leaderCommit > self.commitIndex:
                    self.commitIndex = min(leaderCommit, len(self.log)-1)
                    self.applyLogEntries(self.log[self.lastApplied:self.commitIndex+1])
                    self.lastApplied = self.commitIndex

                reply(msg, type="appendEntries_success", term=self.currentTerm)
            else:
                reply(msg, type="appendEntries_insuccess", term=self.currentTerm)

            self.becomeFollower()
        else:
            reply(msg, type="appendEntries_insuccess", term=self.currentTerm)
    
    #Estas funções têm de ser sempre as últimas a serem invocadas num método (garantir que houve troca)
    def becomeFollower():
        from Follower import Follower
        setActiveClass(Follower(super().getState()))

    def resetElection(self, msg):
        setActiveClass(Candidate(super().getState()))
