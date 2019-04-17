import signal
import time
import sys
import os

from .recovery import params
from .messages.message import Message
from .messages.utils import add_log, get_random_alive_node, send_msg

def crash_detect(my_node):

	while True:
		time.sleep(params.CRASH_DETECT_INTERVAL)
		crashed_nodes = []

		for ip in my_node.resources.keys():
			if (time.time() - my_node.last_heartbeat_ts[ip] > params.CRASH_THRESHOLD):
				if ip == my_node.self_ip:
					for ky in my_node.pids.keys():
						os.kill(my_node.pids[ky], signal.SIGKILL)
					os.kill(my_node.submit_interface_pid, signal.SIGKILL)
					return
					
				crashed_nodes.append(ip)

		if len(crashed_nodes) > 0:
			add_log(my_node, "Crashed nodes : " + ', '.join(crashed_nodes), "INFO")
			for ip in crashed_nodes:
				del my_node.resources[ip]


			flg = False
			resched_job_ids = []
			for ip in crashed_nodes:
				for job in my_node.running_jobs[ip]:
					my_node.leader_jobPQ.put(job)
					my_node.leader_joblist += [job]
					flg = True
					resched_job_ids.append(job.job_id)

				del my_node.running_jobs[ip]

			add_log(my_node, "rescheduling jobs : " + ', '.join(resched_job_ids), "INFO")
			
			if flg:
				os.kill(my_node.pids['matchmaker'], signal.SIGUSR1)

			if my_node.ip_dict['backup'] in crashed_nodes:
				new_backup_ip = get_random_alive_node(my_node.resources, [my_node.ip_dict['root'], my_node.ip_dict['backup']])
				add_log(my_node, "New backup elected with ip " + new_backup_ip, ty = "INFO")

				my_node.ip_dict['backup'] = new_backup_ip

				msg = Message('BACKUP_QUERY',)
				send_msg(msg, to = new_backup_ip, my_node = my_node)

