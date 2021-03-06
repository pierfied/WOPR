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

    def submit_job(self, func, args_list):
        # Create and submit each job to the head.
        job_ids = []
        for args in args_list:
            job_id = np.random.uniform()
            # job = Job(job_id, func, arg)
            job = {'job_id': job_id, 'func': func, 'args': args}
            self.send_msg(pa.serialize(job).to_buffer())

            job_ids.append(job_id)

        return self.get_results(job_ids)

    def get_results(self, job_ids):
        # Get the result for each job from the head and throw an error if an error occurred.
        results_dict = {}
        for i in range(len(job_ids)):
            # Try to receive data and throw an error if disconnected or if an error occurred running the job.
            try:
                # data = self.recv_msg()

                job = pa.deserialize(memoryview(self.recv_msg()))
                # if job['err'] is None:
                #     results_dict[job['job_id']] = job['result']
                # else:
                #     print('Error in job ' + str(job['job_id']))
                #     raise Exception(job['err'])
            except:
                raise Exception('Lost connection to the head.')

            if job['err'] is None:
                results_dict[job['job_id']] = job['result']
            else:
                print('Error in job ' + str(job['job_id']))
                raise Exception(job['err'])

        # Sort the results.
        results = []
        for i in range(len(job_ids)):
            results.append(results_dict[job_ids[i]])

        return results

    def close(self):
        # Close the connection to the head.
        self.head.close()
