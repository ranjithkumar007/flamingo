class Message:
	def __init__(self, msg_type, content):
		self.msg_type = msg_type
		self.content = content
		# contains :
		# root of msg for leader election related messages

	def send_msg(self, to, sock = None):
		if not sock:
			sock = socket.socket()
			sock.connect((to, network_params.CLIENT_RECV_PORT))

		msg_data = io.BytesIO(pickle.dumps(self.msg))

		while True:
			chunk = msg_data.read(network_params.BUFFER_SIZE)

			if not chunk:
				break

			sock.send(chunk)

		sock.shutdown(socket.SHUT_WR)
		sock.close()

	def recv_msg(self, conn):
		data = conn.recv(network_params.MAX_RECV_SIZE)
		data_list = []

		while data:
			data_list.append(data)
			data = conn.recv(network_params.MAX_RECV_SIZE)

		data = b''.join(data_list)
		self.msg = pickle.loads(data)

		return self.msg
