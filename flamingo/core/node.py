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
		
		self.jobQ = None
		self.resources = {}
		self.yet_to_submit = None
		self.last_jobs_sent = 0

		self.root_ip_dict = None
		self.backup_ip_dict = None

		self.log_q = None

		self.running_jobs = None
		self.completed_jobs = {}
		self.leader_jobPQ = None
		# self.matchmaker_pid = None
		self.pids = None

		#proxy for leader_jobPQ to share the leader state with backup
		self.leader_joblist = None

		self.last_heartbeat_ts = None
		# self.crash_detector_pid = None

		# self.logging_pid = None
		self.lost_resources = None
		self.job_pid = {}
		self.submit_interface_pid = None

		self.individual_running_jobs = {}

		self.job_submitted_time = {}


		self.backup_state = []
		self.leader_last_seen = {}

		self.failed_msgs = []
