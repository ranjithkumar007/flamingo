from .message import Message
from .utils import send_msg, get_job_status, start_job, \
				get_random_alive_node, send_file, exec_new_job, add_log, send_heartbeat,\
				sleep_and_ping, get_leaderstate, get_resources, sleep_and_ping_backup
import time
import os
import signal
import random
from . import params
from multiprocessing import Process


def exec_job_handler(my_node, job):
	inp_fp = job.attr['input_path']
	exec_fp = job.attr['exec_path']

	my_node.job_submitted_time[job.job_id] = job.submitted_time

	if not os.path.exists(params.EXEC_DIR):
		os.makedirs(params.EXEC_DIR)

	if not os.path.exists(params.LOG_DIR):
		os.makedirs(params.LOG_DIR)

	dirpath = os.path.join(params.EXEC_DIR, job.job_id)
	if not os.path.exists(dirpath):
		os.makedirs(dirpath)

	my_node.individual_running_jobs[job.job_id] = job

	if job.source_ip == my_node.self_ip:
		os.system("cp " + job.attr['input_path'] + " " + dirpath + "/input")
		os.system("cp " + job.attr['exec_path'] + " " + dirpath + "/executable")
		
		start_job(my_node, job.job_id, job.source_ip)
		return
	# storing job in indiviudual_running_jobs
	

	msg = Message('QUERY_FILES', content = [job.job_id, inp_fp, exec_fp])
	send_msg(msg, to = job.source_ip, my_node = my_node)

def query_files_handler(my_node, recv_ip, content):
	job_id = content[0]
	send_file(content[1], to = recv_ip, job_id = job_id, file_ty = "input", my_node = my_node)
	send_file(content[2], to = recv_ip, job_id = job_id, file_ty = "executable", my_node = my_node)

def get_alive_node_handler(my_node, recv_ip, content):
	not_ip, job_id = content

	ip = get_random_alive_node(my_node.resources, [not_ip])
	msg = Message('GET_ALIVE_NODE_ACK', content = [ip, job_id])

	send_msg(msg, to = recv_ip, my_node = my_node)

def get_alive_node_ack_handler(my_node, content):
	log_ip, job_id = content
	send_file(os.path.join(params.LOG_DIR, job_id), to = log_ip, job_id = job_id, file_ty = "log", my_node = my_node)	

def log_file_handler(my_node, content):
	job_id, file_ty, file_content = content

	log_path = os.path.join(params.LOG_DIR, job_id)

	if not os.path.exists(params.LOG_DIR):
		os.system("mkdir " + params.LOG_DIR)

	with open(log_path, 'wb') as fp:
		fp.write(file_content)

	msg = Message('LOG_FILE_ACK', content = job_id)	
	send_msg(msg, to = my_node.ip_dict['root'], my_node = my_node)

def completed_job_handler(my_node, recv_ip, content):
	job_id, job_run_time, tat = content
	j = None
	for i in range(len(my_node.running_jobs[recv_ip])):
		if my_node.running_jobs[recv_ip][i].job_id == job_id:
			j = i
			break

	if j != None:
		my_node.running_jobs[recv_ip] = my_node.running_jobs[recv_ip][:j] + my_node.running_jobs[recv_ip][j+1:]
	
	if not job_id in my_node.completed_jobs.keys():
		my_node.completed_jobs[job_id] = {}

	my_node.completed_jobs[job_id]['turn_around_time'] = tat
	my_node.completed_jobs[job_id]['job_run_time'] = job_run_time
	my_node.completed_jobs[job_id]['log_file_ip1'] = recv_ip
	
def preempt_and_exec_handler(my_node, to, content):
	preempt_pid = my_node.job_pid[content[1]]

	os.kill(preempt_pid, signal.SIGKILL)
	add_log(my_node, "Preempted this job with id : %s in node %s" % (content[1],my_node.self_ip), "INFO")
	msg = Message('PREEMPTED_JOB',content = [my_node.individual_running_jobs[content[1]]])
	send_msg(msg, to = to, my_node = my_node)
	del my_node.individual_running_jobs[content[1]]
	del my_node.job_pid[content[1]]

	exec_job_handler(my_node, content[0])

def preempted_job_handler(my_node, recv_addr, content):
	my_node.running_jobs[recv_addr].remove(content[0])


def status_job_handler(my_node, recv_addr, content):
	jobid = content[0]

	reply = get_job_status(my_node, jobid)
	
	msg = Message('STATUS_REPLY',content = [jobid, reply])
	send_msg(msg, to = recv_addr, my_node = my_node)

def status_reply_handler(my_node, content):
	jobid = content[0]
	reply = content[1]
	print("Status of job with jobid %s is %s" % (jobid,reply))

	# woke the submit_interface process
	os.kill(my_node.submit_interface_pid, signal.SIGUSR1)
	

def log_file_ack_handler(my_node, recv_ip, content):
	job_id = content
	if not job_id in my_node.completed_jobs.keys():
		my_node.completed_jobs[job_id] = {}
	
	my_node.completed_jobs[job_id]['log_file_ip2'] = recv_ip

def files_content_handler(my_node, recv_ip, content):
	job_id, file_ty, file_content = content

	dirpath = os.path.join(params.EXEC_DIR, job_id)
	file_path = os.path.join(dirpath, file_ty)

	with open(file_path, 'wb') as fp:
		fp.write(file_content)

	if file_ty == "executable":
		os.system("chmod +x " + file_path)

	if os.path.exists(os.path.join(dirpath, "executable")) \
		and os.path.exists(os.path.join(dirpath, "input")):
		start_job(my_node, job_id, recv_ip)

def backup_query_handler(my_node):
	my_node.ip_dict['backup'] = my_node.self_ip
	my_node.leader_last_seen['time'] =  time.time()
	msg = Message('BACKUP_HEARTBEAT')
	send_msg(msg, to = my_node.ip_dict['root'], my_node = my_node)

def elect_new_leader_handler(my_node):
	#need to elect new leader and send the state
	resources = my_node.backup_state[3]
	new_leader_ip = get_random_alive_node(resources, [my_node.ip_dict['root'], my_node.self_ip])

	#remove old leader from resources list
	del my_node.backup_state[3][my_node.ip_dict['root']]

	add_log(my_node, "New leader elected with ip " + new_leader_ip, ty = "INFO")

	msg = Message('U_ARE_LEADER',content = my_node.backup_state)
	send_msg(msg, to = new_leader_ip, my_node = my_node)


def backup_heartbeat_handler(my_node):
	mystate = get_leaderstate(my_node)
	msg = Message('BACKUP_HEARTBEAT_ACK',content = mystate)
	send_msg(msg,to = my_node.ip_dict['backup'], my_node = my_node)

def backup_heartbeat_ack_handler(my_node, content):
	my_node.leader_last_seen['time'] = time.time()
	my_node.backup_state = content
	knocker_p = Process(target = sleep_and_ping_backup, args = (my_node, my_node.ip_dict['root']))
	knocker_p.start()

def backup_elect_handler(my_node):
	my_node.ip_dict['backup'] = my_node.adj_nodes_ips[0]
	msg = Message('BACKUP_QUERY')
	send_msg(msg, to = my_node.ip_dict['backup'], my_node = my_node)

def le_result_handler(my_node):
	add_log(my_node, my_node.self_ip + " is the leader", "INFO")
	my_node.le_elected = True
	my_node.ip_dict['root'] = my_node.self_ip

	msg = Message('LE_TERMINATE')
	for ip in my_node.children:
		send_msg(msg, to = ip, my_node = my_node)

	send_heartbeat(my_node, to = my_node.self_ip)

def heartbeat_ack_handler(my_node):
	for i in range(my_node.last_jobs_sent):
		rid = my_node.jobQ[0].job_id
		del my_node.jobQ[0]
		del my_node.yet_to_submit[rid]

	my_node.last_jobs_sent = 0

def display_output_handler(my_node, recv_ip, content):
	job_id = content[0]
	job_status = get_job_status(my_node, job_id)

	if job_status != "Completed":
		msg = Message('DISPLAY_OUTPUT_ACK',content = [job_id, job_status])
		send_msg(msg, to = recv_ip, my_node = my_node)
		return

	to_addr = None
	
	if ((my_node.completed_jobs[job_id]['log_file_ip1'] == recv_ip or \
		my_node.completed_jobs[job_id]['log_file_ip2'] == recv_ip) and \
		recv_ip in my_node.resources.keys()):
		to_addr = recv_ip
	elif my_node.completed_jobs[job_id]['log_file_ip1'] in my_node.resources.keys():
		to_addr = my_node.completed_jobs[job_id]['log_file_ip1']

	elif my_node.completed_jobs[job_id]['log_file_ip2'] in my_node.resources.keys():
		to_addr = my_node.completed_jobs[job_id]['log_file_ip2']
	else:
		print("Exception!! Flamingo supports only 1 fault. 2 faults detected")
	
	msg = Message('FWD_DISPLAY_OUTPUT', content = [recv_ip, job_id])
	send_msg(msg, to = to_addr, my_node = my_node)

def fwd_display_output_handler(my_node, content):
	source_ip, job_id = content

	send_file(os.path.join(params.LOG_DIR, job_id), to = source_ip, job_id = job_id, file_ty = "fwd_display_output_ack", my_node = my_node)

def display_output_ack_handler(my_node, content):
	print("Job id : %s status : %s; Output can be displayed only after it completes" % (content[0], content[1]))
	os.kill(my_node.submit_interface_pid, signal.SIGUSR1)

def fwd_display_output_ack_handler(my_node, content):
	job_id, file_ty, file_content = content

	print(file_content.decode('UTF-8'))
	os.kill(my_node.submit_interface_pid, signal.SIGUSR1)	

	# see for backup

# both task and resource manager combined
def heartbeat_handler(my_node, recv_ip, content, manager):
	# call matchmaker
	node_jobQ, node_res = content
	my_node.resources[recv_ip] = node_res

	if not recv_ip in my_node.all_ips:
		my_node.all_ips.append(recv_ip)

	my_node.lost_resources[recv_ip] = {'memory' : 0, 'cores' : 0}
	my_node.last_heartbeat_ts[recv_ip] = time.time()

	for job_i in node_jobQ:
		my_node.leader_jobPQ.put(job_i)

	for job_i in node_jobQ:
		my_node.leader_joblist += [job_i]

	if not recv_ip in my_node.running_jobs.keys():
		my_node.running_jobs[recv_ip] = manager.list()

	# wake up matchmaker
	# os.system("pgrep -P " + str(os.getpid()))
	# print(my_node.matchmaker_pid)
	os.kill(my_node.pids['matchmaker'], signal.SIGUSR1)

	msg = Message('HEARTBEAT_ACK')
	send_msg(msg, to = recv_ip, my_node = my_node)

	knocker_p = Process(target = sleep_and_ping, args = (my_node, recv_ip))
	knocker_p.start()
	
def le_terminate_handler(my_node):
	msg = Message('LE_TERMINATE')
	my_node.le_elected = True
	my_node.ip_dict['root'] = my_node.ip_dict['root']

	for ip in my_node.children:
		send_msg(msg, to = ip, my_node = my_node)

	send_heartbeat(my_node, to = my_node.ip_dict['root'])

def le_query_handler(my_node, recv_ip, new_root_ip):
	if not my_node.le_elected and my_node.ip_dict['root'] < new_root_ip: 
		my_node.ip_dict['root'] = new_root_ip
		my_node.par = recv_ip
		my_node.children = []
		my_node.le_acks[my_node.ip_dict['root']] = 0

		msg = Message('LE_QUERY', content = my_node.ip_dict['root'])
		for ip in my_node.adj_nodes_ips:
			if ip != recv_ip:
				send_msg(msg, to = ip, my_node = my_node)

		if len(my_node.adj_nodes_ips) == 1:
			msg = Message('LE_ACCEPT', content = my_node.ip_dict['root'])
			send_msg(msg, to = my_node.par, my_node = my_node)

	elif not my_node.le_elected:
		msg = Message('LE_REJECT', content = my_node.ip_dict['root'])
		send_msg(msg, to = recv_ip, my_node = my_node)
	else:
		msg = Message('LE_FORCE_LEADER', content = my_node.ip_dict['root'])
		send_msg(msg, to = recv_ip, my_node = my_node)		

def le_force_leader_handler(my_node, recv_ip, new_root_ip):
	my_node.le_elected = True
	my_node.ip_dict['root'] = new_root_ip
	my_node.ip_dict['root'] = my_node.ip_dict['root']
	
	send_heartbeat(my_node, to = my_node.ip_dict['root'])

def le_accept_handler(my_node, recv_ip, new_root_ip, is_accept = True):
	if my_node.ip_dict['root'] == new_root_ip:
		if is_accept:
			my_node.children.append(recv_ip)
		my_node.le_acks[my_node.ip_dict['root']] += 1
			
		if my_node.ip_dict['root'] == my_node.self_ip and my_node.le_acks[my_node.ip_dict['root']] == len(my_node.adj_nodes_ips):
			# leader election completed
			backup_elect_handler(my_node)
			# propogate that you are the finally elected leader
			le_result_handler(my_node)
			
		if my_node.ip_dict['root'] != my_node.self_ip and my_node.le_acks[my_node.ip_dict['root']] == (len(my_node.adj_nodes_ips) - 1):
			msg = Message('LE_ACCEPT', content = my_node.ip_dict['root'])
			send_msg(msg, to = my_node.par, my_node = my_node)

def le_reject_handler(my_node, recv_ip, new_root_ip):
	# pass
	return le_accept_handler(my_node, recv_ip, new_root_ip, False)


def new_leader_handler(my_node, recv_ip , content):

	my_node.all_ips = content[0]
	my_node.all_ips.remove(my_node.ip_dict['root'])

	my_node.completed_jobs = content[2]
	resources = content[3]
	for key in resources.keys():
		my_node.resources[key] = resources[key]

	my_node.jobQ += content[4]

	for key in content[5].keys():
		my_node.running_jobs[key] = content[5][key]

	for job in content[6]:
		my_node.leader_jobPQ.put(job)
		my_node.leader_joblist += [job]

	my_node.ip_dict['root'] = my_node.self_ip
	my_node.ip_dict['backup'] = recv_ip

	msg = Message("I_AM_NEWLEADER")

	for ip in my_node.all_ips:
		send_msg(msg, to = ip, my_node = my_node)

def i_am_newleader_handler(my_node,recv_ip):

	my_node.ip_dict['root'] = recv_ip
	my_node.ip_dict['root'] = recv_ip

	#send failed messages

	for msg in my_node.failed_msgs:
		send_msg(msg,to = my_node.ip_dict['root'], my_node = my_node)

	tt = len(my_node.failed_msgs)
	for i in range(tt):
		del my_node.failed_msgs[i]

	send_heartbeat(my_node, to = my_node.ip_dict['root'])

	if my_node.self_ip == my_node.ip_dict['backup']:
		my_node.leader_last_seen['time'] = time.time()
		os.kill(my_node.pids['leader_crash_detector'], signal.SIGUSR1)
