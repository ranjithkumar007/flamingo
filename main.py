import argparse
import socket
import sys
import os
import time
from multiprocessing import Process
from messages import network_params

from jobs.manager import Manager
from jobs.jobqueue import JobPQ
from utils.node import Node
from messages.message import Message
from messages import handlers
from jobs.submit_interface import submit_interface

def build_socket(self_ip):
    msg_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    msg_socket.bind((self_ip, network_params.CLIENT_RECV_PORT))
    msg_socket.listen(network_params.MAX_OUTSTANDING_REQUESTS)

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
    my_node.jobQ = manager.Queue()
    my_node.leaderQ = JobPQ(manager)

    interface_p = Process(target = submit_interface, args = (my_node, newstdin))
    interface_p.start()

    # start receiving messages
    msg_socket = build_socket(self_ip)

    # Leader election
    initiate_leader_election(my_node)
    
    msg = Message()

    while 1:
        conn, recv_addr = msg_socket.accept()

        msg = recv_msg(conn)
        print("received message from %s of type %s ", recv_addr, msg.msg_type)


        if msg.msg_type == 'LE_QUERY':
            hadlers.le_query_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_ACCEPT':
            hadlers.le_accept_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_REJECT':
            hadlers.le_reject_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_TERMINATE':
            hadlers.le_terminate_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_TERMINATE_ACK':
            hadlers.le_terminate_ack_handler(my_node, msg.content)
        elif msg.msg_type == 'BACKUP_QUERY':
            hadlers.backup_query_handler(my_node, recv_addr, msg.content)


        # if my_node.le_elected and start_daemons:
        #     start_daemons = False

        #     if my_node.self_ip == my_node.root_ip: # Leader
        #         collector_p = Process(target=initiate_collector, args=(my_node.all_ips))


if __name__ == '__main__':
    main()
