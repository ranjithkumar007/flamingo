def parse_job_file(filepath):
	with open(filepath, 'r') as fp:
		lines = fp.read().splitlines() 
		specs = {}

		for line in lines:
			attr, val = line.split(' ')
			specs[attr] = val

		return specs

def calculate_job_priority(job_ob):
	