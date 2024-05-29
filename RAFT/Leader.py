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
        for node in node_ids():
            self.nextIndex[node] = self.currentTerm + 1
            self.matchIndex[node] = -1
        
        self.timer.create(lambda: send(node_id(), type="heartbeat"))
        self.timer.start()
            
    
    def heartbeat(self, msg):
        for dest_id in node_ids():
            send(dest_id, type="appendEntries", message=(
                    self.currentTerm, # term
                    node_id(), #leaderId
                    -1, # prevLogIndex
                    self.currentTerm, # prevLogTerm
                    [], # entries[]
                    self.commitIndex) # leaderCommit
                )

    def read(self, msg):

        kv_store =self.kv_store

        if msg.body.key in kv_store:
            reply(msg, type='read_ok', value=kv_store[msg.body.key])
        else:
            reply(msg, type='error', code='20', text='key does not exist')
    
    def write(self, msg):
        self.log.append((msg, self.currentTerm))

        for dest_id in node_ids():
            if len(self.log) >= self.nextIndex[dest_id]:
                send(dest_id, type="appendEntries", message=(
                    self.currentTerm, # term
                    node_id(), #leaderId
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
            for toBeCommited in log[self.commitIndex:candidate]:
                body = self.log[toBeCommited][0].body
                self.kv_store[body.key] = body.value
            self.commitIndex = candidate

    def appendEntries_insuccess(self, msg):
        dest_id = msg.src
        
        self.nextIndex[dest_id] -= 1
        send(dest_id, type="appendEntries", message=(
            self.currentTerm, # term
            node_id(), #leaderId
            self.nextIndex[dest_id]-1, # prevLogIndex
            self.log[self.nextIndex[dest_id]-1][1], # prevLogTerm
            [self.log[i] for i in range(self.nextIndex[dest_id],len(self.log))], # entries[]
            self.commitIndex) # leaderCommit
            )

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        if term > self.currentTerm: # if a valid leader contacts:
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

    def requestVote(self, msg):
        term = msg.body.term

        if term > self.currentTerm:
            self.currentTerm = term

            _, lastLogTerm = self.log[-1]
            if  msg.body.lastLogTerm < lastLogTerm or \
                (msg.body.lastLogTerm == lastLogTerm and msg.body.lastLogIndex < len(self.log)):
                self.votedFor = None
                reply(msg, type='handleVote', term=self.currentTerm, voteGranted=False) #todo: reply false, is it worth tho? in the paper says to reply false
            else:
                self.votedFor = msg.src
                reply(msg, type='handleVote', term=term, voteGranted=True)

            self.becomeFollower()
        else:
            reply(msg, type="handleVote", term=self.currentTerm, voteGranted = False)