import random
import time

keys = ['key1', 'key2']

# Produce a point every 100 ms
with open('in.txt', 'w') as input_file:
    for i in range(1000):
        key = keys[random.randint(0, 1)]
        x = random.randint(0, 100)
        y = random.randint(0, 100)
        input_file.write(f"{key}:{x},{y}\n")
        input_file.flush()
        time.sleep(0.1)
