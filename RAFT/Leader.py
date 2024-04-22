from Follower import *

class Leader(Follower):
    def __init__(self):
        super().__init__()

        # Volatile state
        self.nextIndex = {}
        self.matchIndex = {}
    
    def read(msg):

        kv_store = super.self.kv_store

        if msg.body.key in kv_store:
            reply(msg, type='read_ok', value=kv_store[msg.body.key])
        else:
            reply(msg, type='error', code='20', text='key does not exist')
    
    def write(self, msg):
        super().self.log.append((msg, super.self.currentTerm))

        for dest_id in node_ids():
            if len(log) >= super.self.nextIndex[dest_id]:
                send(dest_id, type="appendEntries", message=(super.self.currentTerm, # term
                                                              super.self.nextIndex[dest_id]-1, # prevLogIndex
                                                              log[super.self.nextIndex[dest_id]-1], # prevLogTerm
                                                              [log[i] for i in range(super.self.nextIndex[dest_id],len(log))], # entries[]
                                                              super.self.commitIndex)) # leaderCommit

    