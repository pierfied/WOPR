import socket
import pyarrow as pa
import numpy as np
from .job import Job
from .message_interface import MessageInterface


class Client(MessageInterface):
    job_ids = []

    def __init__(self, address, port):
        self.address = address
        self.port = port

        # Create the socket and connect to the head.
        self.head = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.head.connect((self.address, self.port))
        super().__init__(self.head)
        self.send_msg(b'CLIENT')

    def submit_job(self, func, args):
        # Create and submit each job to the head.
        for arg in args:
            job_id = np.random.standard_normal()
            job = Job(job_id, func, arg)
            self.send_msg(pa.serialize(job).to_buffer())

            self.job_ids.append(job_id)

        return self.get_results()

    def get_results(self):
        # Get the result for each job from the head.
        results_dict = {}
        for i in range(len(self.job_ids)):
            data = self.recv_msg()
            job = pa.deserialize(memoryview(data))
            results_dict[job.job_id] = job.result

        # Sort the results.
        results = []
        for i in range(len(self.job_ids)):
            results.append(results_dict[self.job_ids[i]])

        return results

    def close(self):
        self.head.close()