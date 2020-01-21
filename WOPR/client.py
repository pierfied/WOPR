import socket
import pyarrow as pa
import numpy as np
from .job import Job
from .message_interface import MessageInterface


class Client(MessageInterface):
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
        job_ids = []
        for arg in args:
            job_id = np.random.standard_normal()
            job = Job(job_id, func, arg)
            self.send_msg(pa.serialize(job).to_buffer())

            job_ids.append(job_id)

        return self.get_results(job_ids)

    def get_results(self, job_ids):
        # Get the result for each job from the head.
        results_dict = {}
        for i in range(len(self.job_ids)):
            data = self.recv_msg()
            job = pa.deserialize(memoryview(data))
            results_dict[job.job_id] = job.result

        # Sort the results.
        results = []
        for i in range(len(job_ids)):
            results.append(results_dict[job_ids[i]])

        return results

    def close(self):
        # Close the connection to the head.
        self.head.close()
