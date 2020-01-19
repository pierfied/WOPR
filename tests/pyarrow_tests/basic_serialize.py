import numpy as np
import pyarrow as pa

x = np.random.standard_normal(100)

buf = pa.serialize(x).to_buffer()

y = pa.deserialize(buf)

print(x)
print(y)
print(np.allclose(x, y))