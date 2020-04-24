""" Generate client configuration for testing
Example usage:
To generate 1000 request
    python3 gen_test.py 1000

"""
import sys
import string
import random

if __name__ == "__main__":
    num = int(sys.argv[1])
    
    with open(f'./src/main/resources/client0.conf', 'w') as f:
        f.write('server:localhost:12345:/tmp/master\n')
    
    with open(f'./src/main/resources/client0.conf', 'a') as f:
        for i in range(num):
            f.write(f'request:SET:test{i}:{random.choice(string.ascii_letters)}:Causal\n')
    
    print(f'Generated {num} requests')
