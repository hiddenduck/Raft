# Raft with Scalability Improved

## Overview
This project focuses on implementing and testing a variant of the Raft consensus algorithm, enhanced with epidemic dissemination (Driftwood). The goal is to distribute the replication effort, traditionally centralized on the leader in the original Raft protocol, by combining epidemic dissemination of AppendEntries requests with decentralized commitIndex advancement.

## Raft

The Raft protocol was implemented following the original paper, with some considerations from the extended version. The technical implementation is based on classes representing the possible states of each node, with functions shared by the superclass SharedState and others overridden in specific cases. Timeouts are managed using threads and locks for concurrency control instead of sending messages to the node itself. This approach avoids introducing latency in Maelstrom, which would negatively affect tests, such as sending heartbeats by the leader.

## Driftwood

### First Extension (3.1)

The first extension of Driftwood uses epidemic dissemination for replication (appendEntries requests). This means the leader doesn't need to perform RPCs to each follower; instead, it can send to a subset, which then propagates to the others. In the algorithm, a random list of followers is traversed circularly in each round of epidemic dissemination. The implementation avoids unnecessary acknowledgment messages between nodes performing gossip and sends failure messages directly to the leader determined by the gossip, who becomes the recognized leader for the current term.

### Second Extension (3.2)

The new version of Raft reduces the number of messages the leader must exchange by sending unconfirmed entries in batches and allowing followers to disseminate these entries. However, the approach remains in the sense that the leader only advances the CommitIndex after receiving confirmation of replication from a majority of followers. To advance the CommitIndex value in a decentralized manner, new data structures were added for epidemic propagation.

The processes use the bit map to determine when a majority has correctly replicated the entry up to NextCommit. Each process should set its bit in the Bitmap to 1 when its log has the entry at NextCommit and the term of the last entry is equal to the current term. There are two functions to update NextCommit and MaxCommit: Update, to update values when a majority is reached in the bit map, and Merge, to combine values received from different processes during epidemic propagation. The algorithm ensures that NextCommit is greater than MaxCommit before and after using the Merge and Update functions.

## Testing

We tested the following cases:

- Direct read is non-linearizable;
- Leader changing;
- A performance comparison between the Raft protocol and this variant, particularly in the presence of different types and quantities of faults.
