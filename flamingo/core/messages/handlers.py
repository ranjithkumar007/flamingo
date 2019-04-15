from .message import Message
from .utils import send_msg, get_resources
from multiprocessing import Process
import time
from .utils import send_file
import os
import signal
from . import params

def start_job(my_node, job_id):
	print("Starting job")
	cmd = "./executable < input > " +  params.LOG_DIR + "/" + job_id 

	exec_p = Process(target = exec_new_job, args = (job_id, cmd, my_node.root_ip))
	exec_p.start()
	my_node.job_pid[job_id] = exec_p.pid

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

	if job.source_ip == my_node.self_ip:
		os.system("cp " + job.attr['input_path'] + " " + dirpath + "/input")
		os.system("cp " + job.attr['exec_path'] + " " + dirpath + "/executable")
		
		start_job(my_node, job.job_id)
		return

	msg = Message('QUERY_FILES', content = [job.job_id, inp_fp, exec_fp])
	send_msg(msg, to = job.source_ip)

def query_files_handler(my_node, recv_ip, content):
	job_id = content[0]
	send_file(content[1], to = recv_ip, job_id = job_id, file_ty = "input")
	send_file(content[2], to = recv_ip, job_id = job_id, file_ty = "executable")

def exec_new_job(job_id, cmd, root_ip):
	os.chdir(os.path.join(params.EXEC_DIR, job_id))
	st_tm = time.time()
	os.system(cmd)
	end_tm = time.time()

	job_run_time = end_tm - st_tm
	tat = end_tm - my_node.job_submitted_time[job_id]

	print("Completed job")

	msg = Message('COMPLETED_JOB', content = [job_id, job_run_time, tat])
	send_msg(msg, to = root_ip)

	del my_node.job_pid[job_id]
	os.system("rm -rf " + os.path.join(params.EXEC_DIR, job_id))
	
	send_file(os.path.join(params.LOG_DIR, job_id), to = recv_ip, job_id = job_id, file_ty = "log")	
	# send leader msg to remove this job from running Q

def log_file_handler(my_node, content):
	job_id, file_ty, file_content = content

	log_path = os.path.join(params.LOG_DIR, job_id)
	with open(log_path, 'wb') as fp:
		fp.write(file_content)

	msg = Message('LOG_FILE_ACK')	
	send_msg(msg, to = my_node.root_ip)

def completed_job_handler(my_node, recv_ip, content):
	job_id, job_run_time, completion_time, tat = content

	del my_node.running_jobs[job_id]
	
	if not job_id in my_node.completed_jobs:
		my_node.completed_jobs[job_id] = {}
	
	my_node.completed_jobs[job_id]['turn_around_time'] = tat
	my_node.completed_jobs[job_id]['job_run_time'] = job_run_time
	my_node.completed_jobs[job_id]['log_file_ip1'] = recv_ip

def log_file_ack_handler(my_node, recv_ip):
	if not job_id in my_node.completed_jobs:
		my_node.completed_jobs[job_id] = {}
	
	my_node.completed_jobs[job_id]['log_file_ip2'] = recv_ip

def files_content_handler(my_node, content):
	job_id, file_ty, file_content = content

	dirpath = os.path.join(params.EXEC_DIR, job_id)
	file_path = os.path.join(dirpath, file_ty)

	with open(file_path, 'wb') as fp:
		fp.write(file_content)

	if file_ty == "executable":
		os.system("chmod +x " + file_path)

	if os.path.exists(os.path.join(dirpath, "executable")) \
		and os.path.exists(os.path.join(dirpath, "input")):
		start_job(my_node, job_id)

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
		rid = my_node.jobQ[0].job_id
		del my_node.jobQ[0]
		del my_node.yet_to_submit[rid]

	my_node.last_jobs_sent = 0

def send_heartbeat(my_node, to):
	cur_res = get_resources()
	my_node.resources[my_node.self_ip] = cur_res

	jobQ_cp = []
	for job_i in my_node.jobQ:
		jobQ_cp.append(job_i)

	msg = Message('HEARTBEAT', content = [jobQ_cp, cur_res])
	print(msg.content[0])
	print(msg.content[1])
	
	my_node.last_jobs_sent = len(msg.content[0])
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

	for job_i in node_jobQ:
		my_node.leader_jobPQ.put(job_i)

	# wake up matchmaker
	os.kill(my_node.matchmaker_pid, signal.SIGUSR1)

	msg = Message('HEARTBEAT_ACK')
	send_msg(msg, to = recv_ip)

	knocker_p = mp.Process(target = sleep_and_ping, args = (recv_ip, ))
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
