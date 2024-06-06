from SharedState import SharedState
from Leader import Leader
from node import *
from bitarray import bitarray

class Candidate(SharedState):
    def __init__(self, sharedState):
        super().__init__()
        # Set State
        super().changeState(sharedState)
        # Volatile State
        self.roundLC = 0
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
            self.node.send(dest_id, type="requestVote", term=self.currentTerm, lastLogIndex = lenLog, lastLogTerm = self.log[-1][1] if lenLog!=0 else 0)
    
    def requestVote(self, msg):
        term = msg.body.term

        if term > self.currentTerm:
            self.timer.stop()

            if  len(self.log) > 0 and \
                (msg.body.lastLogTerm < self.log[-1][1] or \
                (msg.body.lastLogTerm == self.log[-1][1] and msg.body.lastLogIndex < len(self.log))):
                #Não devíamos responder se não vai mudar em nada
                self.node.reply(msg, type='handleVote', term=term, voteGranted=False)
                votedFor = None
            else:
                self.node.reply(msg, type='handleVote', term=term, voteGranted=True)
                votedFor = msg.src

            self.newTerm(term, votedFor)
            self.becomeFollower()
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
            self.newTerm(msg.body.term)
            self.becomeFollower()

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderRound, isRPC, bitlist, maxCommit, nextCommit = tuple(msg.body.message)
        bitmap = bitarray(bitlist)

        if term >= self.currentTerm: # if a valid leader contacts (no candidate é >= no leader é >)
            self.timer.stop()
            self.newTerm(term, votedFor=leaderID)
            self.mergeBitmap(bitmap, maxCommit, nextCommit)
            if (isRPC or leaderRound > self.roundLC):
                if len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex][1] == prevLogTerm):
                    if prevLogIndex >= 0:
                        self.log = self.log[:prevLogIndex+1] + entries
                    else:
                        self.log = entries
                    #sempre que o log muda testa-se o commitindex
                    self.checkBitmap()
                    self.updateBitmap()
                    self.checkCommitIndex()

                    if isRPC:
                        self.node.send(leaderID, type="appendEntries_success", term=self.currentTerm, lastLogIndex=len(self.log)-1)
                    if not isRPC:
                        self.roundLC = leaderRound
                        #TODO Gossip request
                        self.sendEntries(leaderID)
                    
                else:
                    self.log = entries[:prevLogIndex]
                    self.node.send(leaderID, type="appendEntries_insuccess", term=self.currentTerm, lastLogIndex=min(len(self.log)-1, prevLogIndex-1))        

            self.becomeFollower()
        else:
            self.node.send(leaderID, type="appendEntries_insuccess", term=self.currentTerm, lastLogIndex=len(self.log)-1)

    #Estas funções têm de ser sempre as últimas a serem invocadas num método (garantir que houve troca)
    def becomeFollower(self):
        from Follower import Follower
        self.node.setActiveClass(Follower(super().getState()))

    def becomeLeader(self):
        from Leader import Leader
        self.node.setActiveClass(Leader(super().getState()))
