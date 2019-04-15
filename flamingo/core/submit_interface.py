import argparse
import  time
import sys
from .jobs.job import Job
from .jobs.utils import parse_job_file, generate_id

def submit_interface(my_node, newstdin):
	sys.stdin = newstdin
	while True:
		client_id = int(input('\n>>Enter your user id\n'))
		# add authentication

		while True:
			inp = input('\n>>')
			print(inp)
			slots = inp.split(' ')
			command = slots[0]

			if command == "submit_job":

				if (not (len(slots) == 3 and slots[1] == '--filepath')):
					print("Path to job description file is not provided. specify it using --filepath flag")
					continue

				jd = slots[2]

				job_ob = Job()
				job_ob.attr = parse_job_file(jd)
				job_ob.client_id = client_id
				job_ob.source_ip = my_node.self_ip
				# job_ob.submitted_time =  time.time()

				job_ob.job_id = generate_id(my_node.self_ip)
				
				if my_node.self_ip == my_node.root_ip:
					my_node.leader_jobPQ.put(job_ob)
				else:
					my_node.jobQ.append(job_ob)
					my_node.yet_to_submit[job_ob.job_id] = 1
			else :
				pass
