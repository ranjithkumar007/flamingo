import signal
import time
import sys
import os
from .recovery import params 
from .messages.message  import Message
from .messages.utils import add_log, send_msg

def signal_handler(sig, frame):
	if sig == signal.SIGUSR1:
		pass

def leader_crash_detect(my_node):
	signal.signal(signal.SIGUSR1, signal_handler)

	while True:
		time.sleep(params.BACKUP_CRASH_DETECT_INTERVAL)
		
		if (time.time() - my_node.leader_last_seen['time']) > params.BACKUP_CRASH_THRESHOLD:
			add_log(my_node, "Leader crashed", ty = "INFO")

			msg = Message('ELECT_NEW_LEADER')
			send_msg(msg, to = my_node.self_ip, my_node = my_node)

			signal.pause()
