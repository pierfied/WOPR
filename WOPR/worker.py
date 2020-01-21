import socket
import multiprocessing
import pyarrow as pa
import threading
from .message_interface import MessageInterface


class Worker(MessageInterface):
    job_queue = multiprocessing.Queue()
    results_queue = multiprocessing.Queue()
    avail_queue = multiprocessing.Queue()

    def __init__(self, address, port, num_workers):
        self.address = address
        self.port = port
        self.num_workers = num_workers

        # Create the socket and connect to the head.
        self.head = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.head.connect((self.address, self.port))
        super().__init__(self.head)
        self.send_msg(b'WORKER')

        # Start the job manager, availability manager, and results manager.
        threading.Thread(target=self.job_manager).start()
        threading.Thread(target=self.avail_manager).start()
        self.results_manager()

    def job_manager(self):
        # Start the worker processes.
        worker_processes = []
        for i in range(self.num_workers):
            worker_process = multiprocessing.Process(target=self.worker_process)
            worker_process.daemon = True
            worker_process.start()
            worker_processes.append(worker_process)

        # Listen for new jobs.
        while True:
            data = self.recv_msg()
            self.job_queue.put(data)

    def results_manager(self):
        while True:
            result = self.results_queue.get()
            self.send_msg(result)

    def avail_manager(self):
        while True:
            self.avail_queue.get()
            self.send_msg(b'AVAIL')

    def worker_process(self):
        while True:
            # Notify that we're ready to receive jobs.
            self.avail_queue.put(None)

            # Wait for a new job.
            data = self.job_queue.get()
            job = pa.deserialize(memoryview(data))

            # Run the function.
            job['result'] = job['func'](job['args'])

            # Delete the now unnecessary function and arguments.
            del job['func']
            del job['args']

            self.results_queue.put(pa.serialize(job).to_buffer())
