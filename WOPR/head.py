import socket
import pyarrow as pa
import threading
from collections import deque
from .message_interface import MessageInterface


class Head(MessageInterface):
    clients = []
    workers = []
    job_queue = deque()
    avail_workers = deque()
    jobs_in_prog = {}
    assigned_jobs = {}

    def __init__(self, port):
        # Create the server socket.
        self.host = ''
        self.port = port
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        super().__init__(self.server)

        # Start accepting connections from clients and workers.
        self.start_server()

    def start_server(self):
        # Bind the socket.
        self.server.bind((self.host, self.port))

        # Start listening for new connections.
        while True:
            self.server.listen()
            conn, addr = self.server.accept()
            conn = MessageInterface(conn)

            # Check if the new connection is a worker or a client and start a new thread to manage the connection.
            if conn.recv_msg() == b'WORKER':
                self.workers.append(conn)
                print(addr, 'has connected as WORKER.')

                self.assigned_jobs[conn] = []

                thread = threading.Thread(target=self.worker_manager, args=(conn,))
                thread.start()
            else:
                self.clients.append(conn)
                print(addr, 'has connected as CLIENT.')

                thread = threading.Thread(target=self.client_manager, args=(conn,))
                thread.start()

    def run_job_manager(self):
        # Check if a job and worker are available.
        if len(self.job_queue) > 0 and len(self.avail_workers) > 0:
            job_info = self.job_queue.popleft()
            worker = self.avail_workers.popleft()

            # Try to send the job to the worker. If it fails, resubmit the job to the job queue.
            try:
                worker.send_msg(pa.serialize(job_info['job']).to_buffer())
                self.assigned_jobs[worker].append(job_info)
            except:
                self.job_queue.appendleft(job_info)

    def client_manager(self, client):
        # Listen for new jobs from the client and add to the queue.
        while True:
            try:
                # Get the job from the client.
                job = pa.deserialize(memoryview(client.recv_msg()))
            except:
                # If the client disconnects, remove all jobs from the queue that this client created.
                new_job_queue = []
                for i, job_info in enumerate(self.job_queue):
                    if job_info['client'] != client:
                        new_job_queue.append(job_info)
                self.job_queue = deque(new_job_queue)

                # Remove this client's jobs from the dict of jobs in progress.
                for job_id in self.jobs_in_prog.copy():
                    if self.jobs_in_prog[job_id]['client'] == client:
                        del self.jobs_in_prog[job_id]

                # Kill the thread if the client disconnects.
                break

            # Create the info for the job and add to the queue as well as adding to the jobs in progress.
            job_info = {'client': client, 'job': job}
            self.job_queue.append(job_info)
            self.jobs_in_prog[job['job_id']] = job_info

            # Run the job manager to check if the new job can be run.
            self.run_job_manager()

    def worker_manager(self, worker):
        # Listen for messages from the worker.
        while True:
            try:
                # Get the message from the worker.
                msg = worker.recv_msg()

                # Check if the worker is available to receive jobs or is returning a result.
                if msg == b'AVAIL':
                    # If available add to the list of available workers and run the job manager.
                    self.avail_workers.append(worker)
                    self.run_job_manager()
                else:
                    # If returning a result get the client for the corresponding job id and forward the message.
                    job = pa.deserialize(memoryview(msg))
                    job_info = self.jobs_in_prog[job['job_id']]

                    # Pass the result on to the client.
                    # If the client disconnects, just do nothing and delete the returned result.
                    try:
                        job_info['client'].send_msg(msg)
                    except:
                        pass
                    del self.jobs_in_prog[job['job_id']]
                    self.assigned_jobs[worker].remove(job_info)
            except:
                # If the worker disconnects, resubmit all jobs that this worker was working on.
                for job_info in self.assigned_jobs[worker]:
                    if job_info['job']['job_id'] in self.jobs_in_prog:
                        self.job_queue.appendleft(job_info)

                del self.assigned_jobs[worker]

                # Kill the thread if the worker disconnects.
                break
