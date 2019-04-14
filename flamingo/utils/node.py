import queue

class Node:
	def __init__(self, self_ip, adj_nodes_ips):
		self.self_ip = self_ip
		self.le_acks = {self_ip:0}
		self.le_term_acks = 0
		self.root_ip = self_ip
		self.adj_nodes_ips = adj_nodes_ips
		self.children = []
		self.backup_ip = None
		self.all_ips = [self.self_ip]
		self.le_elected = False
		self.par = -1
		
		self.jobQ = queue.Queue()
		self.resources = {}
		self.yet_to_submit = None
		self.last_jobs_sent = 0

		self.running_jobs = None

		self.leader_jobPQ = None
		self.matchmaker_pid = None

