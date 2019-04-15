import socket
import io
import pickle
from . import params
from .message import Message
import psutil
import os
from functools import partial

def get_resources():
	res = {
		'memory' : psutil.virtual_memory().available >> 20,
		'cpu_usage' : psutil.cpu_percent(),
		'cores' : psutil.cpu_count(),
		'process_load' : os.getloadavg()
	}
	return res

def create_socket(to):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	while 1:
		try:
			sock.connect((to, params.CLIENT_RECV_PORT))
			break
		except ConnectionRefusedError:
			pass

	return sock

# check if alive, return true/false
def send_msg(msg, to, sock = None, close_sock = True):
	if not sock:
		sock = create_socket(to)

	msg_data = io.BytesIO(pickle.dumps(msg))

	while True:
		chunk = msg_data.read(params.BUFFER_SIZE)

		if not chunk:
			break

		sock.send(chunk)

	sock.shutdown(socket.SHUT_WR)
	print('sent msg of type %s to %s' % (msg.msg_type, to))
	if close_sock:
		sock.close()

def send_file(filepath, to, job_id, file_ty):
	sock = create_socket(to)

	if file_ty == 'log':
		msg_ty = 'LOG_FILE'
	else:
		msg_ty = 'FILES_CONTENT'

	msg = Message(msg_ty)
	msg.content = [job_id, file_ty]

	data_list = []
	with open(filepath, 'rb') as fp:
	    for chunk in iter(partial(fp.read, 1024), b''):
	    	data_list.append(chunk)

	data = b''.join(data_list)
	msg.content.append(data)

	send_msg(msg, to, sock)

def recv_msg(conn):
	data = conn.recv(params.BUFFER_SIZE)
	data_list = []

	while data:
		data_list.append(data)
		data = conn.recv(params.BUFFER_SIZE)

	data = b''.join(data_list)
	msg = pickle.loads(data)
	assert isinstance(msg, Message), "Received object on socket not of type Message."

	return msg