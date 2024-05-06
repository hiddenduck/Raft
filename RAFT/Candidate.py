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
        self.requestVote()
        self.timer.create(lambda: send(node_id(), type="startElection"))
        self.timer.start()

    def requestVote(self):
        lenLog = len(self.log)
        for dest_id in self.node_ids():
            send(dest_id, type="request_vote", term=self.currentTerm, lastLogIndex = lenLog, lastLogTerm = self.log[-1] if lenLog==0 else -1)
    
    def handleVote(self, msg):
        if msg.body.term == self.currentTerm:
            self.voters.add(msg.src)

            if len(self.voters) >= len(node_ids()) / 2: # case (a): a Candidate received majority of votes
                setActiveClass(Leader())
    
    def becomeFollower():
        setActiveClass(Follower())

    def startElection(self):
        setActiveClass(Candidate(super().getState()))