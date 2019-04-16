import signal
import time
import sys
import os
from . import params 
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
		
		if (time.time() - my_node.leader_last_seen > params.CRASH_THRESHOLD):
			print("Leader crashed")
			#need to elect new leader and send the state

			resources = my_node.backup_state[3]
			new_leader_ip = get_random_alive_node(resources, my_node.root_ip, my_node.self_ip)

			#remove old leader from resources list
			my_node.backup_state[3].remove(my_node.root_ip_dict['ip'])

			print("New leader elected with ip %s",new_leader_ip)

			msg = Message('U_ARE_LEADER',content = my_node.backup_state)

			send_msg(msg,to = new_leader_ip) 





