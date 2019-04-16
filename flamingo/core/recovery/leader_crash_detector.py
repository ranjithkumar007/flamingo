import signal
import time
import sys
import os
from . import params 

def leader_crash_detect(my_node):

	while True:
		time.sleep(params.CRASH_DETECT_INTERVAL)
		
		if (time.time() - my_node.leader_last_seen > params.CRASH_THRESHOLD):
			print("Leader crashed")
			#need to elect new leader and send the state 