import socket
import multiprocessing
import pyarrow as pa
from .message_interface import MessageInterface


class Worker(MessageInterface):
    job_queue = multiprocessing.Queue()

    def __init__(self, address, port, num_workers):
        self.address = address
        self.port = port
        self.num_workers = num_workers

        # Create the socket and connect to the head.
        self.head = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.head.connect((self.address, self.port))
        super().__init__(self.head)
        self.send_msg(b'WORKER')

        # Start the job manager.
        self.job_manager()

    def job_manager(self):
        # Start the worker processes.
        worker_processes = []
        for i in range(self.num_workers):
            worker_process = multiprocessing.Process(target=self.worker_process)
            worker_process.start()
            worker_processes.append(worker_process)

        # Listen for new jobs.
        while True:
            data = self.recv_msg()
            job = pa.deserialize(memoryview(data))
            self.job_queue.put(job)

    def worker_process(self):
        while True:
            # Wait for a new job.
            job = self.job_queue.get()

            # Run the function.
            job.result = job.func(job.args)

            # Send the result back to the head.
            self.send_msg(pa.serialize(job).to_buffer())