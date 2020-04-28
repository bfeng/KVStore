import glob
from pathlib import Path
import re
from itertools import product

def read_logs(path):
    logs = {}
    log_root = Path(path) 
    for p in log_root.glob('worker*.log'):
        with open(p, 'r') as f:
            worker_id = int(p.stem.split('_')[1])
            ms = re.finditer('<<<Message\[(\d+)\]\[(\d+)\] Delivered!>>>', f.read())
            msgs = []
            for m in ms:
                msgs.append(m.groups())
            logs[worker_id] = msgs
    return logs

def validate_sequential():
    logs = read_logs('logs')
    # print(logs)
    keys = list(logs.keys())
    try:
        for i, j in product(keys, keys):
            assert logs[i] == logs[j]
        print("The output obyes the sequential consistency")
    except AssertionError:
        print("The output doesn't obey the sequential consistency")

if __name__ == "__main__":
    validate_sequential()