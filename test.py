import sys
import os
from subprocess import PIPE, Popen, TimeoutExpired, run
from time import sleep
import string
import random
import argparse
import socket


def set_up_arg_parser():
    parser = argparse.ArgumentParser(description='Launch the kv store test')
    parser.add_argument('-w', type=int,
                        help='the number of workers')
    parser.add_argument('-t', type=int,
                        help='the number of requests')
    parser.add_argument('-m', type=str, choices=['Sequential', 'Causal', 'Eventual', 'Linear'], nargs='*',
                        help='generate messages with different consistency model. Multiple options are allowed')
    return parser


def gen_tests_case(test_num, mode, ports):
    """ Generate client configuration for testing
    Example usage:
    To generate 1000 request
        python3 gen_test.py 1000

    """
    with open(f'./src/main/resources/client0.conf', 'w') as f:
        f.write(f'server:localhost:{ports.pop()}:/tmp/master\n')

    with open(f'./src/main/resources/client0.conf', 'a') as f:
        for i in range(test_num):
            f.write(
                f'request:SET:test{i}:{random.choice(string.ascii_letters)}:{random.choice(mode)}\n')

    print(f'Generated {test_num} requests...')


def kill_all(pool):
    for p in pool:
        p.kill()


def get_free_tcp_port(worker_num):
    ports = []
    conns = []
    for i in range(worker_num):
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conns.append(tcp)
        tcp.bind(('', 0))
        addr, port = tcp.getsockname()
        ports.append(port)
    for conn in conns:
        conn.close()
    return ports


def gen_workers_config(worker_num, ports):
    with open(f'./src/main/resources/servers.conf', 'w') as f:
        f.write(f'master:localhost:{ports.pop()}:/tmp/master\n')

    with open(f'./src/main/resources/servers.conf', 'a') as f:
        for i in range(worker_num):
            f.write(f'worker:localhost:{ports.pop()}:/tmp/worker{i}\n')
    print(f'Generated {worker_num} workers...')


def launch(worker_num, mode):
    # Compile before preceeding
    run(['mvn', 'clean', 'package'])

    pool = []
    master = Popen(['java', '-cp', 'target/KVStore-1.0-SNAPSHOT-jar-with-dependencies.jar',
                    'kvstore.servers.Master', 'src/main/resources/servers.conf'])
    pool.append(master)
    sleep(1)

    for i in range(worker_num):
        worker = Popen(['java', '-cp', 'target/KVStore-1.0-SNAPSHOT-jar-with-dependencies.jar',
                        'kvstore.servers.Worker', 'src/main/resources/servers.conf', str(i)])
        sleep(1)
        pool.append(worker)

    sleep(1)

    # Launch client
    worker = Popen(['java', '-cp', 'target/KVStore-1.0-SNAPSHOT-jar-with-dependencies.jar',
                    'kvstore.client.KVClient', 'src/main/resources/client0.conf', str(0)])

    try:
        outs, errs = master.communicate(timeout=60)
    except TimeoutExpired:
        kill_all(pool)
        outs, errs = master.communicate()


if __name__ == "__main__":
    parser = set_up_arg_parser()
    args = parser.parse_args()
    ports = get_free_tcp_port(args.w+1)  # the one more for the master
    gen_tests_case(args.t, args.m, ports[:])
    gen_workers_config(args.w, ports[:])
    launch(args.w, args.m)
