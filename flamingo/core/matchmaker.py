import signal
import sys
from .messages.message import Message
from .messages.utils import send_msg, get_resources

def signal_handler(sig, frame):
	if sig == signal.SIGUSR1:
		pass

# decrement resources
def match(job, resources, running_jobs):
	# greedy approach
	mem = job.attr['max_memory']
	cores = job.attr['cores']
	assigned_ip = None
	preempt_job_id = None
	for ip in resources.keys():
		if resources[ip]['memory'] >= mem and resources[ip]['cores'] >= cores:
			assigned_ip = ip
			return assigned_ip, preempt_job_id

	return assigned_ip, preempt_job_id

def matchmaking(my_node):
	my_node.resources[my_node.self_ip] = get_resources()
	signal.signal(signal.SIGUSR1, signal_handler)

	while True:
		signal.pause()

		poor_jobs = []
		
		while not my_node.leader_jobPQ.empty():
			job = my_node.leader_jobPQ.get()
			assigned_ip, preempt_job_id = match(job, my_node.resources, my_node.running_jobs)

			if assigned_ip and not preempt_job_id:
				msg = Message('EXEC_JOB',content=job)
				send_msg(msg,to = assigned_ip)
			if assigned_ip and preempt_job_id:	
				msg = Message('PREEMPT_AND_EXEC',content=[job, preempt_job_id])
				send_msg(msg,to = assigned_ip)
			if not assigned_ip and not preempt_job_id:
				poor_jobs.append(job)

		for job in poor_jobs:
			my_node.leader_jobPQ.put(job)
