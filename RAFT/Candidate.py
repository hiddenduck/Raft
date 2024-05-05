import SharedState
import Follower
import Leader
from node import *

class Candidate(SharedState):
    def __init__(self, node_id, node_ids):
        super().__init__(node_id, node_ids)
        self.voters = {node_id}
        self.votedFor = node_id
        self.currentTerm += 1
        self.requestVote()

    def requestVote(self):
        lenLog = len(self.log)
        for dest_id in self.node_ids:
            send(dest_id, type="request_vote", term=self.currentTerm, lastLogIndex = lenLog, lastLogIndex = self.log[-1] if lenLog==0 else -1)
    
    def handleVote(self, msg):
        if msg.body.term == self.currentTerm:
            self.voters.add(msg.src)

            if len(self.voters) >= len(node_ids) / 2: # case (a): a Candidate received majority of votes
                return Leader()
    
    def becomeFollower():
        return Follower(node_id, node_ids).changeState(super.getState())

    def startElection(self):
        self.timer.reset()
        self.voters = {node_id}
        self.currentTerm += 1
        self.requestVote()