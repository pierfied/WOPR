import numpy as np
import WOPR

address = 'localhost'
port = 12345

client = WOPR.Client(address, port)


def f(x):
    np.random.seed(x)
    return np.random.standard_normal()


num_samps = 100

results = np.array(client.submit_job(f, [i for i in range(num_samps)]))

local_results = np.array([f(i) for i in range(num_samps)])

print(np.allclose(results, local_results))
