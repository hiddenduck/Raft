from SharedState import SharedState
from node import *

class Leader(SharedState):
    def __init__(self, sharedState):
        super().__init__()
        # set State
        super().changeState(sharedState)
        # Volatile state
        self.nextIndex = {}
        self.matchIndex = {}
        for node in self.node.node_ids():
            self.nextIndex[node] = self.currentTerm + 1
            self.matchIndex[node] = -1
        
        self.node.send(self.node.node_id(), type="heartbeat")
        self.timer.create(lambda: self.node.send(self.node.node_id(), type="heartbeat"))
        self.timer.start()
            
    
    def heartbeat(self, msg):
        for dest_id in self.node.node_ids():
            self.node.send(dest_id, type="appendEntries", message=(
                    self.currentTerm, # term
                    self.node.node_id(), #leaderId
                    -1, # prevLogIndex
                    self.currentTerm, # prevLogTerm
                    [], # entries[]
                    self.commitIndex) # leaderCommit
                )

    def read(self, msg):

        kv_store =self.kv_store

        if msg.body.key in kv_store:
            self.node.reply(msg, type='read_ok', value=kv_store[msg.body.key])
        else:
            self.node.reply(msg, type='error', code='20', text='key does not exist')
    
    def write(self, msg):
        self.log.append((msg, self.currentTerm))

        for dest_id in self.node.node_ids():
            if len(self.log) >= self.nextIndex[dest_id]:
                self.node.send(dest_id, type="appendEntries", message=(
                    self.currentTerm, # term
                    self.node.node_id(), #leaderId
                    self.nextIndex[dest_id]-1, # prevLogIndex
                    self.log[self.nextIndex[dest_id]-1][1], # prevLogTerm
                    [self.log[i] for i in range(self.nextIndex[dest_id],len(self.log))], # entries[]
                    self.commitIndex) # leaderCommit
                )
                
    def appendEntries_success(self, msg):
        self.nextIndex[msg.src] = msg.body.nextIndex
        self.matchIndex[msg.src] = msg.body.nextIndex

        # Detect Majority

        count = 0
        candidate = None

        for replica in self.matchIndex.keys():
            if count == 0:
                candidate = self.matchIndex[replica]
                count = 1
            else:
                count = count+1 if self.matchIndex[replica] == candidate else count-1
        
        count = 0

        for replica in self.matchIndex.keys():
            if self.matchIndex[replica] == candidate:
                count += 1
        
        if count > len(self.matchIndex.keys())/2 and candidate > self.commitIndex:
            for toBeCommited in self.log[self.commitIndex:candidate]:
                body = self.log[toBeCommited][0].body
                self.kv_store[body.key] = body.value
            self.commitIndex = candidate

    def appendEntries_insuccess(self, msg):
        dest_id = msg.src
        
        self.nextIndex[dest_id] -= 1
        self.node.send(dest_id, type="appendEntries", message=(
            self.currentTerm, # term
            self.node.node_id(), #leaderId
            self.nextIndex[dest_id]-1, # prevLogIndex
            self.log[self.nextIndex[dest_id]-1][1], # prevLogTerm
            [self.log[i] for i in range(self.nextIndex[dest_id],len(self.log))], # entries[]
            self.commitIndex) # leaderCommit
            )

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        success = False
        changeType = False

        if term > self.currentTerm: # if a valid leader contacts:
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
            self.node.reply(msg, type="handleVote", term=self.currentTerm, voteGranted = False)