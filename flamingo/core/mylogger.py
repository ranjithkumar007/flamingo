from .messages.utils import create_logger
import os

def start_logger(log_q, log_file, log_level):
	mylogger = create_logger(log_filename = log_file)

	os.system("terminator -e \"echo Presenting you logging details; exec zsh\"; tail -f main_log_data.txt;")

	while 1:
		signal.pause()

		while not log_q.empty():
			ty, item = log_q.get()
			if ty == "INFO":
				mylogger.info(item)
			elif ty == "DEBUG":
				mylogger.debug(item)

