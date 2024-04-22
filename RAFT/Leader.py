from Follower import *

class Leader(Follower):
    def __init__(self):
        super().__init__()

        # Volatile state
        self.nextIndex = {}
        self.matchIndex = {}
    
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
                send(dest_id, type="appendEntries", message=(self.currentTerm, # term
                                                              self.nextIndex[dest_id]-1, # prevLogIndex
                                                              self.log[self.nextIndex[dest_id]-1], # prevLogTerm
                                                              [self.log[i] for i in range(self.nextIndex[dest_id],len(self.log))], # entries[]
                                                              self.commitIndex)) # leaderCommit

    
