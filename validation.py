import glob
from pathlib import Path
import re
from itertools import combinations
import argparse
import pprint

def set_up_arg_parser():
    parser = argparse.ArgumentParser(description='validate the result')
    parser.add_argument('-m', type=str, choices=['Sequential', 'Causal', 'Eventual', 'Linear'],
                        help='the consistency model')
    return parser

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
        for i, j in combinations(keys, 2):
            res = logs[i] == logs[j]
            print(f'Check worker {i} and worke {j}...{res}')
            if not res:
                pprint.pprint(logs[i])
                pprint.pprint(logs[j])
                raise AssertionError
        print("The output obyes the sequential consistency")
    except AssertionError:
        print("The output doesn't obey the sequential consistency")

def validate(mode):
    if mode == 'Sequential':
        validate_sequential()

if __name__ == "__main__":
    parser = set_up_arg_parser()
    args = parser.parse_args()
    validate(args.m)