import socket
import io
import pickle
import network_params

class Message:
	def __init__(self, msg_type = None, content = None):
		self.msg_type = msg_type
		self.content = content
		# contains :
		# root of msg for leader election related messages

	def send_msg(self, to, sock = None):
		if not sock:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			while 1:
				try:
					sock.connect((to, network_params.CLIENT_RECV_PORT))
					break
				except ConnectionRefusedError:
					pass

		msg_data = io.BytesIO(pickle.dumps(self))

		while True:
			chunk = msg_data.read(network_params.BUFFER_SIZE)

			if not chunk:
				break

			sock.send(chunk)

		sock.shutdown(socket.SHUT_WR)
		sock.close()
		print("SENT BRO")

	def recv_msg(self, conn):
		data = conn.recv(network_params.BUFFER_SIZE)
		data_list = []

		while data:
			data_list.append(data)
			data = conn.recv(network_params.BUFFER_SIZE)

		data = b''.join(data_list)
		self.msg = pickle.loads(data)

		return self.msg
