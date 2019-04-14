import time

class Job:
	def __init__(self):
		self.client_id = None
		self.attr = {}
		self.job_id = None
		self.source_ip = None
		self.submitted_time = time.time()
		
		