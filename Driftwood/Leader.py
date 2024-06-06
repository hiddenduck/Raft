from SharedState import SharedState
from node import *

class Leader(SharedState):
    def __init__(self, sharedState):
        super().__init__()
        # set State
        super().changeState(sharedState)
        # Volatile state
        self.roundLC = 0
        self.nextIndex = {}
        self.matchIndex = {}
        for node in self.node.node_ids():
            self.nextIndex[node] = len(self.log)
            self.matchIndex[node] = -1
        
        self.sendEntries(True)
        self.timer.a = 0.05
        self.timer.b = 0.05
        self.timer.create(lambda s: s.heartbeat(), self)
        self.timer.start()
    
    def sendEntries(self, isRPC=False):
        self.roundLC += 1
        #TODO tornar este envio num gossip só para alguns
        for dest_id in self.node.node_ids():
            if len(self.log) >= self.nextIndex[dest_id]:
                self.node.send(dest_id, type="appendEntries", message=(
                    self.currentTerm, # term
                    self.node.node_id(), #leaderId
                    self.commitIndex, # prevLogIndex
                    self.log[self.commitIndex+1][1] if self.commitIndex+1 >= 0 else -1, # prevLogTerm
                    [self.log[i] for i in range(self.commitIndex+1,len(self.log))], # entries[]
                    self.commitIndex, # leaderCommit
                    self.roundLC, #leaderRound
                    isRPC, #isRPC
                    self.bitarray, #bitmap
                    self.maxCommit, #maxCommit
                    self.nextCommit #nextCommit
                    ) 
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
        self.nextIndex[msg.src]  = msg.body.lastLogIndex+1
        self.matchIndex[msg.src] = msg.body.lastLogIndex

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

        if candidate == self.commitIndex:
            return

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
        lastLogIndex = msg.body.lastLogIndex

        if msg.body.term <= self.currentTerm:
            self.node.send(dest_id, type="appendEntries", message=(
                self.currentTerm, # term
                self.node.node_id(), #leaderId
                lastLogIndex, # prevLogIndex
                self.log[lastLogIndex][1] if lastLogIndex >= 0 else -1, # prevLogTerm
                [self.log[i] for i in range(lastLogIndex+1,len(self.log))], # entries[]
                self.commitIndex, # leaderCommit
                self.roundLC, #leaderRound
                False, #isRPC
                self.bitarray, #bitmap
                self.maxCommit, #maxCommit
                self.nextCommit #nextCommit
                )
                )
        else:
            self.timer.stop()
            self.newTerm(msg.body.term)
            self.becomeFollower()


    def applyLogEntries(self, entries):
        for (msg, _) in entries:
            self.kv_store[msg.body.key] = msg.body.value

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit, leaderRound, isRPC, bitmap, maxCommit, nextCommit = tuple(msg.body.message)
        success = False
        changedType = False

        if term > self.currentTerm: # if a valid leader contacts (no candidate é >= no leader é >)
            self.timer.stop()
            changedType = True
            self.newTerm(term, votedFor=leaderID)
            self.merge(bitmap, maxCommit, nextCommit)
            if (isRPC or leaderRound > self.roundLC):
                if len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex][1] == prevLogTerm):
                    success = True
                    if prevLogIndex >= 0:
                        self.log = self.log[:prevLogIndex+1] + entries
                    else:
                        self.log = entries
                    
                else:
                    self.log = entries[:prevLogIndex]

                #sempre que o log muda testa-se o commitindex
                self.updateCommitIndex()

        if not success:
            self.node.send(leaderID, type="appendEntries_insuccess", term=self.currentTerm, lastLogIndex=min(len(self.log)-1, prevLogIndex-1))
        elif not isRPC:
            self.roundLC = leaderRound
            #TODO Gossip request
            undefined
        else:
            self.node.reply(msg, type="appendEntries_success", term=self.currentTerm, lastLogIndex=len(self.log)-1)
            
        if changedType:
            self.becomeFollower()

    def requestVote(self, msg):
        term = msg.body.term

        if term > self.currentTerm:
            self.timer.stop()

            if  len(self.log) > 0 and \
                (msg.body.lastLogTerm < self.log[-1][1] or \
                (msg.body.lastLogTerm == self.log[-1][1] and msg.body.lastLogIndex < len(self.log))):
                #Não devíamos responder se não vai mudar em nada
                self.node.reply(msg, type='handleVote', term=term, voteGranted=False) #todo: reply false, is it worth tho? in the paper says to reply false
                votedFor = None
            else:
                self.node.reply(msg, type='handleVote', term=term, voteGranted=True)
                votedFor = msg.src

            self.newTerm(term, votedFor)
            self.becomeFollower()
        else:
            self.node.reply(msg, type="handleVote", term=self.currentTerm, voteGranted = False)

    def becomeFollower(self):
        from Follower import Follower
        self.timer.a = 0.150
        self.timer.b = 0.300
        self.node.setActiveClass(Follower(super().getState()))