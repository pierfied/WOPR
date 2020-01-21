import struct
import threading


class MessageInterface:
    def __init__(self, socket):
        self.socket = socket
        self.send_lock = threading.Lock()
        self.recv_lock = threading.Lock()

    def send_msg(self, msg):
        with self.send_lock:
            # Prefix each message with a 4-byte length (network byte order)
            msg = struct.pack('>I', len(msg)) + msg
            self.socket.sendall(msg)

    def recv_msg(self):
        with self.recv_lock:
            # Read message length and unpack it into an integer
            raw_msglen = self.recvall(4)
            if not raw_msglen:
                return None
            msglen = struct.unpack('>I', raw_msglen)[0]
            # Read the message data
            return self.recvall(msglen)

    def recvall(self, n):
        # Helper function to recv n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < n:
            packet = self.socket.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data
