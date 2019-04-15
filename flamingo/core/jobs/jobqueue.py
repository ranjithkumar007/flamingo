from .utils import calculate_job_priority

class JobPQ:
	def __init__(self, manager):
		self.pq = manager.PriorityQueue()

	def empty(self):
		return self.pq.empty()

	def get(self):
		if self.pq.empty():
			return None

		return self.pq.get()[1]

	def put(self, job_ob):
		job_priority = calculate_job_priority(job_ob) * -1
		
		self.pq.put((job_priority, job_ob))
