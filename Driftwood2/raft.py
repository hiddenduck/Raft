#!/usr/bin/env python
import logging
from node import *
import Follower

logging.getLogger().setLevel(logging.DEBUG)

node = None

@handler
def init(msg):
    global node

    node_id = msg.body.node_id
    node_ids = msg.body.node_ids
    logging.info('node %s initialized', node_id)

    node = Follower(node_id, [id for id in node_ids if id != node_id] if node_ids != None else None)

    logging.info('Follower Created')

    reply(msg, type='init_ok')

@handler
def read(msg):
    node.read(msg)

@handler
def write(msg):
    node.write(msg)

@handler
def cas(msg):
    node.cas(msg)

if __name__ == "__main__":
    receive()