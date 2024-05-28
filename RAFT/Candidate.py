from SharedState import SharedState
from Follower import Follower
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
        self.timer.create(lambda: send(node_id(), type="startElection"))
        self.timer.start()

    def startRequestVote(self):
        lenLog = len(self.log)
        for dest_id in node_ids():
            send(dest_id, type="requestVote", term=self.currentTerm, lastLogIndex = lenLog, lastLogTerm = self.log[-1][1] if lenLog!=0 else 0)
    
    def requestVote(self, msg):
        candidateID = msg.src
        term = msg.body.term

        if term > self.currentTerm:
            self.currentTerm = term
            self.votedFor = None
            if self.log[msg.body.lastLogIndex] != msg.body.lastLogTerm: 
                reply(msg, type='handleVote', term=self.currentTerm, voteGranted=False) #todo: reply false, is it worth tho? in the paper says to reply false
            else:
                self.votedFor = candidateID
                reply(msg, type="handleVote", term=msg.body.term, voteGranted = True)

            self.becomeFollower()
        else:
            reply(msg, type="handleVote", term=self.currentTerm, voteGranted = False)

    def handleVote(self, msg):
        if msg.body.term == self.currentTerm:
            self.voters.add(msg.src)

            if len(self.voters) >= len(node_ids()) / 2: # case (a): a Candidate received majority of votes
                setActiveClass(Leader())

        elif msg.body.term > self.currentTerm:
            self.currentTerm = msg.body.term
            self.becomeFollower()


    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        if term >= self.currentTerm: # if a leader a valid leader contacts:
            if len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex] == prevLogTerm):
                self.currentTerm = term
                self.log = log[:prevLogIndex+1] + entries
                
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
        setActiveClass(Follower(super().getState()))

    def startElection(self):
        setActiveClass(Candidate(super().getState()))
