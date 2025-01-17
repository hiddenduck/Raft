from SharedState import SharedState
from node import *

class Leader(SharedState):
    def __init__(self, sharedState):
        super().__init__()
        # set State
        super().changeState(sharedState)
        # Volatile state
        self.votedFor = None
        self.nextIndex = {}
        self.matchIndex = {}
        for node in self.node.node_ids():
            self.nextIndex[node] = len(self.log)
            self.matchIndex[node] = -1
        
        self.sendEntries()
        self.timer.a = 0.05
        self.timer.b = 0.05
        self.timer.create(lambda s: s.heartbeat(), self)
        self.timer.start()
    
    def sendEntries(self):
        for dest_id in self.node.node_ids():
            if len(self.log) >= self.nextIndex[dest_id]:
                self.node.send(dest_id, type="appendEntries", message=(
                    self.currentTerm, # term
                    self.node.node_id(), #leaderId
                    self.nextIndex[dest_id]-1, # prevLogIndex
                    self.log[self.nextIndex[dest_id]-1][1] if self.nextIndex[dest_id]-1 >= 0 else -1, # prevLogTerm
                    [self.log[i] for i in range(self.nextIndex[dest_id],len(self.log))], # entries[]
                    self.commitIndex) # leaderCommit
                )

    def heartbeat(self):
        self.node.log('Heartbeat Sent')
        lock = self.lock
        with lock:
            if self.node.active_class == self:
                self.sendEntries()
            self.timer.reset()

    def read(self, msg):
        kv_store =self.kv_store

        if msg.body.key in kv_store:
            self.node.reply(msg, type='read_ok', value=kv_store[msg.body.key])
        else:
            self.node.reply(msg, type='error', code='20', text='key does not exist')

    def read_redirect(self, msg):
        self.read(msg.body.msg)
    
    def write(self, msg):
        self.log.append((msg, self.currentTerm))
        #self.sendEntries()
    
    def write_redirect(self, msg):
        self.write(msg.body.msg)

    def checkCas(self, newFrom, key, log):
        for entry in reversed(log):
            if entry[0].body.key == key:
                if entry[0].body.value == newFrom:
                    return True
                else:
                    return False

    def cas(self, msg):
        if msg.body.key not in self.kv_store:
            self.node.reply(msg, type='error', code='20', text='key does not exist')
        elif self.checkCas(getattr(msg.body, 'from'), msg.body.key, self.log):
            msg.body.value = msg.body.to # in order to reuse write
            self.write(msg)
        else:
            self.node.reply(msg, type='error', code='22', text='value has changed')

    def cas_redirect(self, msg):
        self.cas(msg.body.msg)
                
    def appendEntries_success(self, msg):
        self.nextIndex[msg.src]  = msg.body.nextIndex
        self.matchIndex[msg.src] = msg.body.nextIndex-1

        # Detect Majority

        count = 1
        candidate = None

        for replica in self.matchIndex.keys():
            if count == 1:
                candidate = self.matchIndex[replica]
                count = 2
            else:
                count = count+1 if self.matchIndex[replica] == candidate else count-1
        
        if candidate == self.commitIndex:
            return

        count = 1

        for replica in self.matchIndex.keys():
            if self.matchIndex[replica] == candidate:
                count += 1
        
        if  count > len(self.matchIndex.keys())/2.0 and \
            candidate > self.commitIndex and self.log[candidate][1] == self.currentTerm:
            for toBeCommited in self.log[self.commitIndex+1:candidate+1]:
                body = toBeCommited[0].body
                self.kv_store[body.key] = body.value
                self.node.reply(toBeCommited[0], type="write_ok" if body.type == "write" else "cas_ok")
            self.commitIndex = candidate

    def appendEntries_insuccess(self, msg):
        dest_id = msg.src
        
        if msg.body.term <= self.currentTerm:
            self.nextIndex[dest_id] = self.nextIndex[dest_id] - 1 if self.nextIndex[dest_id] > 0 else 0
            self.node.send(dest_id, type="appendEntries", message=(
                self.currentTerm, # term
                self.node.node_id(), #leaderId
                self.nextIndex[dest_id]-1, # prevLogIndex
                self.log[self.nextIndex[dest_id]-1][1] if self.nextIndex[dest_id]-1 >= 0 else -1, # prevLogTerm
                [self.log[i] for i in range(self.nextIndex[dest_id],len(self.log))], # entries[]
                self.commitIndex) # leaderCommit
                )
        else:
            self.timer.stop()
            self.currentTerm = msg.body.term
            self.becomeFollower()


    def applyLogEntries(self, entries):
        for (msg, _) in entries:
            self.kv_store[msg.body.key] = msg.body.value

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

        success = False
        changeType = False

        if term > self.currentTerm: # if a valid leader contacts:
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
            else:
                self.votedFor = msg.src
                self.node.reply(msg, type='handleVote', term=term, voteGranted=True)

            self.becomeFollower()
        else:
            self.node.reply(msg, type="handleVote", term=self.currentTerm, voteGranted = False)

    def becomeFollower(self):
        from Follower import Follower
        self.timer.a = 0.150
        self.timer.b = 0.300
        self.node.setActiveClass(Follower(super().getState()))