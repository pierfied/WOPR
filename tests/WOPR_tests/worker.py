import WOPR

address = 'localhost'
port = 12345
num_workers = 4

head = WOPR.Worker(address, port, num_workers)