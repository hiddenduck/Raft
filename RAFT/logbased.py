#!/usr/bin/env python

from node import *
import node

# Commited log entries
values = {}

# Persistent state, all servers
currentTerm = -1
votedFor = None
log = []

# Volatile state, all servers
commitIndex = -1
lastApplied = -1

# Volatile state, leader
nextIndex = {}
matchIndex = {}

# Message format -> (leaderID, prevLogIndex, entries[], leaderCommit)

@handler
def init(msg):
    node.init(msg)
    for dest_id in node_ids():
        nextIndex[dest_id] = 0
        matchIndex[dest_id] = -1

@handler
def write(msg):
    if node_id() != 0:
        reply(msg, type='error', code='11', text='not the leader')
    else:
        log.append((msg, currentTerm))

        for dest_id in node_ids():
            if len(log) >= nextIndex[dest_id]:
                send(dest_id, type="appendEntries", message=(currentTerm, # term
                                                              nextIndex[dest_id]-1, # prevLogIndex
                                                              log[nextIndex[dest_id]-1], # prevLogTerm
                                                              [log[i] for i in range(nextIndex[dest_id],len(log))], # entries[]
                                                              commitIndex)) # leaderCommit

@handler
def appendEntries(msg):
    global currentTerm, commitIndex, log
    leaderID = msg.src
    leaderTerm, prevLogIndex, prevLogTerm, entries, leaderCommit = tuple(msg.body.message)

    if leaderID == node_ids()[0]:
        if leaderTerm >= currentTerm and log[prevLogIndex] == prevLogTerm:
            for i,entry in enumerate(entries):
                if i+prevLogIndex < len(log):
                    log[i+prevLogIndex] = entry
                else:
                    log.append(entry)
            
            if leaderCommit > commitIndex:
                commitIndex = min(leaderCommit, len(log))
            
            reply(msg, type="appendEntries_success", nextIndex=len(log))
        else:
            reply(msg, type="appendEntries_insuccess")

@handler
def appendEntries_success(msg):
    global nextIndex, matchIndex, log
    nextIndex[msg.src] = msg.body.nextIndex
    matchIndex[msg.src] = msg.body.nextIndex

    # Detect Majority

    count = 0
    candidate = None

    for replica in matchIndex.keys():
        if count == 0:
            candidate = matchIndex[replica]
            count = 1
        else:
            count = count+1 if matchIndex[replica] == candidate else count-1
    
    count = 0

    for replica in matchIndex.keys():
        if matchIndex[replica] == candidate:
            count += 1
    
    if count > len(matchIndex.keys())/2 and candidate > commitIndex:
        for toBeCommited in log[commitIndex:candidate]:
            body = log[toBeCommited][0].body
            values[body.key] = body.value
        commitIndex = candidate


@handler
def appendEntries_insuccess(msg):
    global nextIndex, currentTerm, log
    dest_id = msg.src
    
    nextIndex[dest_id] -= 1
    send(dest_id, type="appendEntries", message=(currentTerm, # term
                                                              nextIndex[dest_id]-1, # prevLogIndex
                                                              log[nextIndex[dest_id]-1], # prevLogTerm
                                                              [log[i] for i in range(nextIndex[dest_id],len(log))], # entries[]
                                                              commitIndex)) # leaderCommit

@handler
def read(msg):
    if node_id() != 0:
        reply(msg, type='error', code='11', text='not the leader')

@handler
def cas(msg):
    reply(msg, type='error', code='10', text='unsupported')

@handler
def read(msg):
    reply(msg, type='error', code='10', text='unsupported')  

if __name__ == "__main__":
    receive()
