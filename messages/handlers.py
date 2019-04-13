from .message import Message
import multiprocessing as mp
import time

def backup_query_handler(my_node):
	my_node.backup_ip = my_node.self_ip

def backup_elect_handler(my_node):
	my_node.backup_ip = my_node.adj_nodes_ips[0]
	msg = Message('BACKUP_QUERY')
	msg.send_msg(to = my_node.backup_ip)

def le_result_handler(my_node):
	print("le first pass done")
	my_node.le_elected = True
	msg = Message('LE_TERMINATE')
	for ip in my_node.children:
		msg.send_msg(to = ip)

def heartbeat_ack_handler(my_node):
	for i in range(my_node.last_jobs_sent):
		temp = my_node.jobQ.get()
		rid = temp.job_id
		del my_node.yet_to_submit[rid]

	my_node.last_jobs_sent = 0

def send_heartbeat(my_node, to):
	msg = Message('HEARTBEAT', content = [my_node.jobQ, my_node.resources])
	my_node.last_jobs_sent = msg.content[0].count()
	msg.send_msg(to)

def sleep_and_ping(to):
	time.sleep(params.HEARTBEAT_INTERVAL)
	msg = Message('ARE_YOU_ALIVE')
	msg.send_msg(to)

def heartbeat_handler(my_node, recv_ip, content):
	# call matchmaker
	# for 

	msg = Message('HEARTBEAT_ACK')
	msg.send_msg(to = recv_ip)

	knocker_p = mp.Process(target = sleep_and_ping, args = (recv_ip))
	knocker_p.start()
	
def le_terminate_handler(my_node):
	msg = Message('LE_TERMINATE')
	my_node.le_elected = True
	for ip in my_node.children:
		msg.send_msg(to = ip)

	send_heartbeat(my_node, to = my_node.root_ip)

def le_query_handler(my_node, recv_ip, new_root_ip):
	if not my_node.le_elected and my_node.root_ip < new_root_ip: 
		my_node.root_ip = new_root_ip
		my_node.par = recv_ip
		my_node.children = []
		my_node.le_acks = 0

		msg = Message('LE_QUERY', content = my_node.root_ip)
		for ip in my_node.adj_nodes_ips:
			if ip != recv_ip:
				msg.send_msg(to = ip)

		if len(my_node.adj_nodes_ips) == 1:
			msg = Message('LE_ACCEPT', content = my_node.root_ip)
			msg.send_msg(to = my_node.par)

	else:
		msg = Message('LE_REJECT', content = my_node.root_ip)
		msg.send_msg(to = recv_ip)

def le_accept_handler(my_node, recv_ip, new_root_ip, is_accept = True):
	if my_node.root_ip == new_root_ip:
		if is_accept:
			my_node.children.append(recv_ip)
		
		my_node.le_acks += 1

		if my_node.le_acks == (len(my_node.adj_nodes_ips) - 1):
			if my_node.root_ip == my_node.self_ip:
				# leader election completed
				backup_elect_handler(my_node)
				# propogate that you are the finally elected leader
				le_result_handler()
			else:
				msg = Message('LE_ACCEPT', content = my_node.root_ip)
				msg.send_msg(to = my_node.par)

def le_reject_handler(my_node, recv_ip, new_root_ip):
	return le_accept_handler(my_node, recv_ip, new_root_ip, False)
