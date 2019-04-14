from .message import Message
from .utils import send_msg, get_resources
import multiprocessing as mp
import time
from .utils import send_file
import os

def exec_job_handler(my_node, job):
	inp_fp = job.attr['input_path']
	exec_fp = job.attr['exec_path']

	if not os.path.exists(params.EXEC_DIR):
		os.makedirs(params.EXEC_DIR)

	if not os.path.exists(os.path.join(params.EXEC_DIR, job.job_id)):
		os.makedirs(os.path.join(params.EXEC_DIR, job.job_id))

	msg = Message('QUERY_FILES', content = [job.job_id, inp_fp, exec_fp])
	send_msg(msg, to = job.source_ip)

def query_files_handler(my_node, recv_ip, content):
	job_id = content[0]
	send_file(content[2], to = recv_ip, job_id = job_id, file_ty = "input")
	send_file(content[1], to = recv_ip, job_id = job_id, file_ty = "executable")
	
def files_content_handler(my_node, content):
	job_id, file_ty, file_content = content

	with open(os.path.join(os.path.join(params.EXEC_DIR, job.job_id), file_ty), 'wb') as fp:
		fp.write(file_content)

def backup_query_handler(my_node):
	my_node.backup_ip = my_node.self_ip

def backup_elect_handler(my_node):
	my_node.backup_ip = my_node.adj_nodes_ips[0]
	msg = Message('BACKUP_QUERY')
	send_msg(msg, to = my_node.backup_ip)

def le_result_handler(my_node):
	print(my_node.self_ip, " is the leader")
	my_node.le_elected = True
	msg = Message('LE_TERMINATE')
	for ip in my_node.children:
		send_msg(msg, to = ip)

def heartbeat_ack_handler(my_node):
	for i in range(my_node.last_jobs_sent):
		temp = my_node.jobQ.get()
		rid = temp.job_id
		del my_node.yet_to_submit[rid]

	my_node.last_jobs_sent = 0

def send_heartbeat(my_node, to):
	my_node.resources[my_node.self_ip] = get_resources()
	
	msg = Message('HEARTBEAT', content = [my_node.jobQ, my_node.resources[my_node.self_ip]])
	my_node.last_jobs_sent = msg.content[0].qsize()
	send_msg(msg, to)

def sleep_and_ping(to):
	time.sleep(params.HEARTBEAT_INTERVAL)
	msg = Message('ARE_YOU_ALIVE')
	send_msg(msg, to)

# both task and resource manager combined
def heartbeat_handler(my_node, recv_ip, content):
	# call matchmaker
	node_jobQ, node_res = content
	my_node.resources[recv_ip] = node_res

	while not node_jobQ.empty():
		my_node.leader_jobPQ.put(node_jobQ.get())

	

	msg = Message('HEARTBEAT_ACK')
	send_msg(msg, to = recv_ip)

	knocker_p = mp.Process(target = sleep_and_ping, args = (recv_ip))
	knocker_p.start()
	
def le_terminate_handler(my_node):
	msg = Message('LE_TERMINATE')
	my_node.le_elected = True
	for ip in my_node.children:
		send_msg(msg, to = ip)

	send_heartbeat(my_node, to = my_node.root_ip)

def le_query_handler(my_node, recv_ip, new_root_ip):
	if not my_node.le_elected and my_node.root_ip < new_root_ip: 
		my_node.root_ip = new_root_ip
		my_node.par = recv_ip
		my_node.children = []
		my_node.le_acks[my_node.root_ip] = 0

		msg = Message('LE_QUERY', content = my_node.root_ip)
		for ip in my_node.adj_nodes_ips:
			if ip != recv_ip:
				send_msg(msg, to = ip)

		if len(my_node.adj_nodes_ips) == 1:
			msg = Message('LE_ACCEPT', content = my_node.root_ip)
			send_msg(msg, to = my_node.par)

	else:
		msg = Message('LE_REJECT', content = my_node.root_ip)
		send_msg(msg, to = recv_ip)

def le_accept_handler(my_node, recv_ip, new_root_ip, is_accept = True):
	if my_node.root_ip == new_root_ip:
		if is_accept:
			my_node.children.append(recv_ip)
		my_node.le_acks[my_node.root_ip] += 1
			
		if my_node.root_ip == my_node.self_ip and my_node.le_acks[my_node.root_ip] == len(my_node.adj_nodes_ips):
			# leader election completed
			backup_elect_handler(my_node)
			# propogate that you are the finally elected leader
			le_result_handler(my_node)
			
		if my_node.root_ip != my_node.self_ip and my_node.le_acks[my_node.root_ip] == (len(my_node.adj_nodes_ips) - 1):
			msg = Message('LE_ACCEPT', content = my_node.root_ip)
			send_msg(msg, to = my_node.par)

def le_reject_handler(my_node, recv_ip, new_root_ip):
	# pass
	return le_accept_handler(my_node, recv_ip, new_root_ip, False)
