import signal
import sys
from .messages.message import Message
from .messages.utils import send_msg, get_resources
from .jobs.utils import calculate_job_priority

MAX_RUNNING_JOBS_PER_NODE = 5
COEFF_MIGRATION_COST = 5
COEFF_CPU_USAGE = 5
COEFF_AVG_PROC_LOAD = 5
COEFF_FREE_MEM = 2000

def signal_handler(sig, frame):
	if sig == signal.SIGUSR1:
		pass

def calc_matching_score(resources, num_running_jobs, is_source):
	total_cost = (1 - is_source) * COEFF_MIGRATION_COST + resources['cpu_usage'] * COEFF_CPU_USAGE \
					+ resources['process_load'] * COEFF_AVG_PROC_LOAD + \
						(1.0 / (resources['memory'] - mem)) * COEFF_FREE_MEM


	return total_cost

# decrement resources
def match(job, resources, running_jobs, lost_resources):
	mem = job.attr['max_memory']
	cores = job.attr['cores']
	source_ip = job.source_ip
	
	assigned_ip = None
	preempt_job_id = None
	
	my_job_p = calculate_job_priority(job)

	preempt_job = {}

	candidate_ips = []

	get_mem = lambda ip : resources[ip]['memory'] - lost_resources[ip]['memory']
	get_cores = lambda ip: resources[ip]['cores'] - lost_resources[ip]['cores']

	for ip in resources.keys():
		if get_mem(ip) >= mem and get_cores(ip) >= cores and len(running_jobs[ip]) < MAX_RUNNING_JOBS_PER_NODE:
			candidate_ips.append(ip)

	for ip in resources.keys():
		min_job = None
		min_job_p = my_job_p

		for job in running_jobs[ip]:
			job_p = calculate_job_priority(job)

			if job_p < min_job_p and (get_mem(ip) + job.attr['max_memory']) >= mem and \
						(get_cores(ip) + job.attr['cores']) >= cores:
				min_job_p = job_p
				min_job = job

		if min_job:
			candidate_ips.append(ip)
			preempt_job[ip] = min_job


	if len(candidate_ips) > 0:
		best_score = 999999999
		for ip in candidate_ips:
			temp = resources[ip]
			temp['memory'] = get_mem(ip)
			temp['cores'] = get_cores(ip)

			num_running_jobs = len(running_jobs[ip])

			if preempt_job != {}:
				temp['memory'] += preempt_job[ip].attr['max_memory']
				temp['cores'] += preempt_job[ip].attr['cores']
				num_running_jobs -=1

			cur_score = calc_matching_score(temp, len(running_jobs[ip]), ip == source_ip)

			if cur_score < best_score:
				best_score = cur_score
				assigned_ip = ip

	if assigned_ip:
		lost_resources[assigned_ip]['memory'] += mem
		lost_resources[assigned_ip]['cores'] += cores

		if preempt_job_id:
			lost_resources[assigned_ip]['memory'] += preempt_job[assigned_ip].attr['max_memory']
			lost_resources[assigned_ip]['cores'] += preempt_job[assigned_ip].attr['cores']

	return assigned_ip, preempt_job_id

def matchmaking(my_node):
	my_node.resources[my_node.self_ip] = get_resources()
	signal.signal(signal.SIGUSR1, signal_handler)

	while True:
		signal.pause()

		poor_jobs = []
		
		while not my_node.leader_jobPQ.empty():
			job = my_node.leader_jobPQ.get()
			assigned_ip, preempt_job_id = match(job, my_node.resources, my_node.running_jobs, my_node.lost_resources)

			if assigned_ip and not preempt_job_id:
				msg = Message('EXEC_JOB',content=job)
				send_msg(msg,to = assigned_ip)

				my_node.running_jobs[assigned_ip] = my_node.running_jobs[assigned_ip] + [job]
				# print(my_node.running_jobs)

			if assigned_ip and preempt_job_id:	
				msg = Message('PREEMPT_AND_EXEC',content=[job, preempt_job_id])
				send_msg(msg,to = assigned_ip)

				if not assigned_ip in my_node.running_jobs.keys():
					my_node.running_jobs[assigned_ip] = []

				my_node.running_jobs[assigned_ip].append(job)

			if not assigned_ip and not preempt_job_id:
				poor_jobs.append(job)

		for job in poor_jobs:
			my_node.leader_jobPQ.put(job)
