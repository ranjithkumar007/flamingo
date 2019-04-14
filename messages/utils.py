import socket
import io
import pickle
from . import network_params
from .message import Message

def send_msg(msg, to, sock = None):
	if not sock:
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		while 1:
			try:
				sock.connect((to, network_params.CLIENT_RECV_PORT))
				break
			except ConnectionRefusedError:
				pass

	msg_data = io.BytesIO(pickle.dumps(msg))

	while True:
		chunk = msg_data.read(network_params.BUFFER_SIZE)

		if not chunk:
			break

		sock.send(chunk)

	sock.shutdown(socket.SHUT_WR)
	sock.close()
	print("sent message to %s of type %s" % (to, msg.msg_type))

def recv_msg(conn):
	data = conn.recv(network_params.BUFFER_SIZE)
	data_list = []

	while data:
		data_list.append(data)
		data = conn.recv(network_params.BUFFER_SIZE)

	data = b''.join(data_list)
	msg = pickle.loads(data)
	assert isinstance(msg, Message), "Received object on socket not of type Message."

	return msg