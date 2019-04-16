import signal
import time
import sys
import os
from .recovery import params 
from .messages.message  import Message
from .messages import utils 

def get_random_alive_node(resources,old_leader_ip,not_ip=None):
	while 1:
		ip = random.choice(resources.keys())
		if ip != not_ip and ip != old_leader_ip:
			return ip

def leader_crash_detect(my_node):

	while True:
		time.sleep(params.CRASH_DETECT_INTERVAL)
		
		if (time.time() - my_node.leader_last_seen) > params.CRASH_THRESHOLD:
			add_log(my_node, "Leader crashed", ty = "INFO")

			msg = Message('ELECT_NEW_LEADER')
			send_msg(msg, to = my_node.self_ip)
