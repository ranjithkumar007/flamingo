import argparse
import socket
import sys
import os
import time
from multiprocessing import Process
import messages.params

from messages.matchmaker import matchmaking
from messages.utils import send_msg, recv_msg
from jobs.manager import Manager
from jobs.jobqueue import JobPQ
from utils.node import Node
from messages.message import Message
from messages import handlers
from jobs.submit_interface import submit_interface

def build_socket(self_ip):
    msg_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    msg_socket.bind((self_ip, messages.params.CLIENT_RECV_PORT))
    msg_socket.listen(messages.params.MAX_OUTSTANDING_REQUESTS)

    return msg_socket


def initiate_leader_election(my_node):
    msg = Message('LE_QUERY', content = my_node.self_ip)
    for ip in my_node.adj_nodes_ips:
        send_msg(msg, to = ip)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--self_ip", help = "Ip of this node", required = True, type = str)
    parser.add_argument("--adj_nodes_path", help = "Path to a list of ips of adjacent nodes", required = True, type = str)
    
    args = vars(parser.parse_args())

    self_ip = args['self_ip']
    adj_nodes_path = args['adj_nodes_path']
    
    adj_nodes_ips = None
    with open(adj_nodes_path, 'r') as f:
        adj_nodes_ips = f.read().splitlines()


    my_node = Node(self_ip, adj_nodes_ips)

    newstdin = os.fdopen(os.dup(sys.stdin.fileno()))
    manager = Manager()

    my_node.yet_to_submit = manager.dict()
    my_node.jobQ = manager.list()
    my_node.resources = manager.dict()
    my_node.running_jobs = manager.dict()
    my_node.leader_jobPQ = JobPQ(manager)

    interface_p = Process(target = submit_interface, args = (my_node, newstdin))
    interface_p.start()

    my_node.matchmaker_pid = Process(target = matchmaking, args = (my_node, ))
    my_node.matchmaker_pid.start()

    # start receiving messages
    msg_socket = build_socket(self_ip)

    # Leader election
    initiate_leader_election(my_node)
    
    msg = Message()

    while 1:
        conn, recv_addr = msg_socket.accept()
        recv_addr = recv_addr[0]
        msg = recv_msg(conn)
        print('received msg of type %s from %s' %(msg.msg_type, recv_addr))

        if msg.msg_type == 'LE_QUERY':
            handlers.le_query_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_ACCEPT':
            handlers.le_accept_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_REJECT':
            handlers.le_reject_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_TERMINATE':
            handlers.le_terminate_handler(my_node)
        elif msg.msg_type == 'BACKUP_QUERY':
            handlers.backup_query_handler(my_node)
        elif msg.msg_type == 'EXEC_JOB':
            handlers.exec_job_handler(my_node)
        elif msg.msg_type == 'QUERY_FILES':
            handlers.query_files_handler(my_node)
        elif msg.msg_type == 'HEARTBEAT':
            handlers.heartbeat_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'FILES_CONTENT':
            handlers.files_content_handler(my_node, msg.content)

        # if my_node.le_elected and start_daemons:
        #     start_daemons = False

        #     if my_node.self_ip == my_node.root_ip: # Leader
        #         collector_p = Process(target=initiate_collector, args=(my_node.all_ips))


if __name__ == '__main__':
    main()
