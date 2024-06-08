from SharedState import SharedState
from node import *
from bitarray import bitarray

class Leader(SharedState):
    def __init__(self, sharedState):
        super().__init__()
        # set State
        super().changeState(sharedState)
        # Volatile state
        self.roundLC = 1
        self.nextIndex = {}
        self.matchIndex = {}
        for node in self.node.node_ids():
            self.nextIndex[node] = len(self.log)
            self.matchIndex[node] = -1
        
        self.sendEntries(self.node.node_id())
        self.timer.a = 0.05
        self.timer.b = 0.05
        self.timer.create(lambda s: s.heartbeat(), self)
        self.timer.start()

    def heartbeat(self):
        self.node.log('Heartbeat Sent')
        lock = self.lock
        with lock:
            if self.node.active_class == self:
                self.roundLC += 1
                self.sendEntries(self.node.node_id())
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
        self.nextCommit = len(self.log)-1
        self.checkBitmap()
        self.updateBitmap()
        self.checkCommitIndex()
    
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
        True

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
                #self.commitIndex, # leaderCommit
                self.roundLC, #leaderRound
                True, #isRPC
                self.bitarray.to01(), #bitmap
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
            self.node.reply(msg, type="write_ok" if msg.body.type == "write" else "cas_ok")

    def appendEntries(self, msg):
        term, leaderID, prevLogIndex, prevLogTerm, entries, leaderRound, isRPC, bitlist, maxCommit, nextCommit = tuple(msg.body.message)
        bitmap = bitarray(bitlist)

        if term > self.currentTerm: # if a valid leader contacts (no candidate é >= no leader é >)
            self.timer.stop()
            self.newTerm(term, votedFor=leaderID)
            self.mergeBitmap(bitmap, maxCommit, nextCommit)
            if (isRPC or leaderRound > self.roundLC):
                if len(self.log) > prevLogIndex and (prevLogIndex < 0 or self.log[prevLogIndex][1] == prevLogTerm):
                    if prevLogIndex >= 0:
                        self.log = self.log[:prevLogIndex+1] + entries
                    else:
                        self.log = entries

                    self.nextCommit = min(len(self.log)-1, nextCommit)
                    self.checkBitmap()
                    self.updateBitmap()
                    self.checkCommitIndex()

                    if isRPC:
                        self.node.send(leaderID, type="appendEntries_success", term=self.currentTerm, lastLogIndex=len(self.log)-1)
                    if not isRPC:
                        self.roundLC = leaderRound
                        #TODO Gossip request
                        self.sendEntries(leaderID, prevLogIndex=prevLogIndex)
                    
                else:
                    self.roundLC = leaderRound
                    self.log = self.log[:prevLogIndex]
                    self.node.send(leaderID, type="appendEntries_insuccess", term=self.currentTerm, lastLogIndex=min(len(self.log)-1, prevLogIndex-1))

            self.becomeFollower()
        elif term == self.currentTerm:
            self.mergeBitmap(bitmap, maxCommit, nextCommit)
            self.updateBitmap()
        else:
            self.node.send(leaderID, type="appendEntries_insuccess", term=self.currentTerm, lastLogIndex=len(self.log)-1)

    def requestVote(self, msg):
        term = msg.body.term

        if term > self.currentTerm:
            self.timer.stop()

            if  len(self.log) > 0 and \
                (msg.body.lastLogTerm < self.log[-1][1] or \
                (msg.body.lastLogTerm == self.log[-1][1] and msg.body.lastLogIndex < len(self.log)-1)):
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