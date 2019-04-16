from .messages.utils import create_logger
import os
import signal

def signal_handler(sig, frame):
	if sig == signal.SIGUSR1:
		pass

def start_logger(log_q, log_file, log_level):
	signal.signal(signal.SIGUSR1, signal_handler)
	if os.path.exists(log_file):
		os.system("rm " + log_file)
	
	mylogger = create_logger(log_filename = log_file)

	# gnome-terminal
	# os.system("terminator -e \"echo Presenting you logging details;  tail -f main_log_data.txt;\"")

	while 1:
		signal.pause()
		while not log_q.empty():
			ty, item = log_q.get()
			if ty == "INFO":
				mylogger.info(item)
			elif ty == "DEBUG":
				mylogger.debug(item)

