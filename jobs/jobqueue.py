class JobPQ:
	def __init__():
		self.pq = None


	def get(self):
		if self.pq.empty():
			return None

		return self.pq.get()


	def put(self, job_ob):
		job_priority = job_ob.attr['priority'] * -1
		self.pq.put(job_ob)

