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
        if msg.body.term > self.currentTerm:
            self.currentTerm = msg.body.term
            self.becomeFollower()

    def handleVote(self, msg):
        if msg.body.term == self.currentTerm:
            self.voters.add(msg.src)

            if len(self.voters) >= len(node_ids()) / 2: # case (a): a Candidate received majority of votes
                setActiveClass(Leader())
        elif msg.body.term > self.currentTerm:
            self.currentTerm = msg.body.term
            setActiveClass(Follower(super().getState()))


    def appendEntries(self, msg):
        term, _, _, _, _, _ = tuple(msg.body.message)

        if term >= self.currentTerm: # if a leader a valid leader contacts:
            self.becomeFollower(super().getState(), msg)
    
    def becomeFollower():
        setActiveClass(Follower(super().getState()))

    def startElection(self):
        setActiveClass(Candidate(super().getState()))
