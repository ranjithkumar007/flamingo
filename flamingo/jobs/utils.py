import time
from . import params

def parse_job_file(filepath):
	with open(filepath, 'r') as fp:
		lines = fp.read().splitlines() 
		specs = {}

		for line in lines:
			attr, val = line.split(' ')
			specs[attr] = val

		return specs

def get_user_priority(client_id):
	return client_id % 10

def calculate_job_priority(job_ob):
	job_age = int(time.time() - job_ob.submitted_time)
	jp_user = job_ob.attr['priority'] # truncate to 10
	userp = get_user_priority(job_ob.client_id)

	total_priority = params.coeff_age * job_age + params.coeff_jp_user * jp_user \
						+ params.coeff_userp * userp

	return total_priority
