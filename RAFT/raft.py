#!/usr/bin/env python
import logging
from concurrent.futures import ThreadPoolExecutor
from ms import send, receiveAll, reply, exitOnError
import Follower

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

def handle(msg):
    global node

    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)

        node = Follower(node_id, [id for id in node_ids if id != node_id] if node_ids != None else None)

        logging.info('Follower Created')

        reply(msg, type='init_ok')
    else:
        node.handle(msg)


executor.map(lambda msg: exitOnError(handle, msg), receiveAll())