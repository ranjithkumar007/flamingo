import signal
import time
import sys
import os
from . import params 

def crash_detect(my_node):

	while True:
		time.sleep(params.CRASH_DETECT_INTERVAL)
		crashed_nodes = []

		for ip in my_node.resources.keys():
			if (time.time() - my_node.last_heartbeat_ts[ip] > params.CRASH_THRESHOLD):
				crashed_nodes.append(ip)

		print("Crashed nodes : ", crashed_nodes)
		for ip in crashed_nodes:
			del my_node.resources[ip]


		print("rescheduling jobs")
		flg = False
		for ip in crashed_nodes:
			for job in my_node.running_jobs[ip]:
				my_node.leader_jobPQ.put(job)
				flg = True

			del my_node.running_jobs[ip]

		if flg:
			os.kill(my_node.matchmaker_pid, signal.SIGUSR1)
