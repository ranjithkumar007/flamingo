import argparse
import  time
import sys
from .jobs.job import Job
from .jobs.utils import parse_job_file, generate_id
from .messages.message import Message
from .messages.utils import send_msg
import signal

def signal_handler(sig, frame):
	if sig == signal.SIGUSR1:
		pass

def submit_interface(my_node, newstdin):
	sys.stdin = newstdin
	signal.signal(signal.SIGUSR1, signal_handler)

	while True:
		client_id = int(input('\n>>Enter your user id : '))
		# add authentication

		while True:
			inp = input('\n>>')
			# print(inp)
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

				print("JobID of the job you just submitted is %s" % (job_ob.job_id, ))

				my_node.jobQ.append(job_ob)
				my_node.yet_to_submit[job_ob.job_id] = 1
			
			elif command == "status":
				if len(slots) == 1:
					print("Jobid is not given")
					continue

				jobid = slots[1]
				msg = Message('STATUS_JOB',content = [jobid])
				send_msg(msg, to = my_node.root_ip_dict['ip'], my_node = my_node)
				# Pause this process until response from leader arrives
				signal.pause()
					
			elif command == "display_output" : #works only when job is completed
				if len(slots) == 1:
					print("Jobid is not given")
					continue

				job_id = slots[1]
				msg = Message('DISPLAY_OUTPUT', content = [job_id])
				send_msg(msg, to = my_node.root_ip_dict['ip'], my_node = my_node)

				signal.pause()
				pass


