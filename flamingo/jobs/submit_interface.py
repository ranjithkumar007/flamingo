import argparse
from utils import *
import  time
import uuid
import sys
from .job import Job

def generate_id(self_ip):
	return hash(self_ip + str(uuid.uuid4()))

def submit_interface(my_node, newstdin):
	sys.stdin = newstdin
	while True:
		client_id = input('\n>>Enter your user id\n')
		# add authentication

		while True:
			inp = input('\n>>')
			print(inp)
			slots = inp.split(' ')
			command = slots[0]

			if command == "submit_job":

				assert len(slots) == 3 and slots[1] == '--filepath', "Path to job description file is not provided. specify it using --filepath flag"
				jd = slots[2]

				job_ob = Job()
				job_ob.attr = parse_job_file(jd)
				job_ob.client_id = client_id
				job_ob.source_ip = my_node.self_ip
				job_ob.submitted_time =  time.time()

				job_ob.job_id = generate_id(my_node.self_ip)
				my_node.jobQ.put(job_ob)
				my_node.yet_to_submit[job_ob.job_id] = 1
			else :
				pass
