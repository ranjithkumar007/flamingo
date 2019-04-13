import argparse
import socket
import sys
import os
from multiprocessing import Process, Manager
from . import network_params

from .node import Node
from ..messages.message import Message
from ..jobs.submit_interface import submit_interface

def build_socket():
    msg_socket = socket.socket()
    
    msg_socket.bind((' ', network_params.CLIENT_RECV_PORT))
    msg_socket.listen(network_params.MAX_OUTSTANDING_REQUESTS)

    return msg_socket


def initiate_leader_election(my_node):
    msg = Message('LE_QUERY', content = my_node.self_ip)
    for ip in my_node.adj_nodes_ips:
        msg.send_msg(to = ip)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--self_ip", help = "Ip of this node", required = True, type = str)
    parser.add_argument("--adj_nodes_path", help = "Path to a list of ips of adjacent nodes", required = True, type = str)
    
    args = vars(parser.parse_args())

    self_ip = args['self_ip']
    adj_nodes_path = args['adj_nodes_path']
    
    adj_nodes_ips = None
    with open(adj_nodes_path, 'r') as f:
        adj_nodes_ips = f.readlines()


    my_node = Node(self_ip, adj_nodes_ips)

    newstdin = os.fdopen(os.dup(sys.stdin.fileno()))
    manager = Manager()

    my_node.yet_to_submit = manager.dict()
    # my_node.begin_times = manager.dict()
    my_node.jobQ = manager.Queue()

    interface_p = Process(target = submit_interface, args = (my_node, newstdin))
    interface_p.start()

    # Leader election
    initiate_leader_election(my_node)

    # start receiving messages
    msg_socket = build_socket()
    
    while True:
        conn, recv_addr = msg_socket.accept()

        msg = recv_msg(conn)
        
        assert isinstance(msg, Message), "Received object on socket not of type Message."

        if msg.msg_type == 'LE_QUERY':
            message_handlers.le_query_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_ACCEPT':
            message_handlers.le_accept_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_REJECT':
            message_handlers.le_reject_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_TERMINATE':
            message_handlers.le_terminate_handler(my_node, recv_addr, msg.content)
        elif msg.msg_type == 'LE_TERMINATE_ACK':
            message_handlers.le_terminate_ack_handler(my_node, msg.content)
        elif msg.msg_type == 'BACKUP_QUERY':
            message_handlers.backup_query_handler(my_node, recv_addr, msg.content)


        if my_node.is_elected and start_daemons:
            start_daemons = False

            if my_node.self_ip == my_node.root_ip: # Leader
                collector_p = Process(target=initiate_collector, args=(my_node.all_ips))


if __name__ == '__main__':
    main()
