import os
import pickle
import socket
import subprocess
import tempfile
import time
from multiprocessing import Process

import psutil
import requests
import redis_server
from redis import Redis

from distributed_computing.client import run_pool_client
from distributed_computing.globals import DEFAULT_PORT, SERVER_PROCESS_NAME, CLIENT_PROCESS_NAME
from distributed_computing.server import run_pool_server
from distributed_computing.common import RedisQueue


def start_redis(password):

    try:
        Redis('localhost', password=password, socket_connect_timeout=1).ping()
    except:
        _, conf_file = tempfile.mkstemp()

        with open(conf_file, 'w') as f:
            f.write(f'requirepass {password}')

        FNULL = open(os.devnull, 'w')

        print('Redis server is started.')

        return subprocess.Popen([redis_server.REDIS_SERVER_PATH, conf_file, '--protected-mode no'], stdout=FNULL)


def start_node(password, address='localhost', port=DEFAULT_PORT, violent_exit=True):

    if is_client_running():
        print('A client is already running on this machine')
        return lambda: 0, lambda: 0

    address = address or 'localhost'

    print('Starting workers pool client.')

    client = Process(target=run_pool_client, args=(address, port, password, violent_exit))
    client.start()

    return lambda: client.terminate(), lambda: client.join()


def start_server(password, port=DEFAULT_PORT):

    if is_server_running():
        print('A server is already running on this machine')
        return lambda: 0, lambda: 0

    redis_process = start_redis(password)

    print('Starting workers pool server.')
    pool_server = Process(target=run_pool_server, args=(port, password))
    pool_server.start()

    def terminate():
        pool_server.terminate()

        if redis_process is not None:
            redis_process.terminate()

    return terminate, lambda: pool_server.join()


def start_head_node(password, port=DEFAULT_PORT):

    server_terminator, server_waiter = start_server(password, port)
    client_terminator, client_waiter = start_node(password, port=port)

    def terminate():
        server_terminator()
        client_terminator()

    def wait():
        server_waiter()
        client_waiter()

    print('\n' + '\033[36m*\033[0m' * 90)
    print('\033[1mTo start a workers client in another node, type:\033[0m')
    ip = requests.get('https://api.ipify.org').text

    print(f'\033[92mpython Experiments/common/distributed_computing/start_worker.py -p {port} -a {ip}\033[0m')
    print('\033[36m*\033[0m' * 90 + '\n')

    return terminate, wait


def is_server_running():
    return SERVER_PROCESS_NAME in [p.name() for p in psutil.process_iter()]


def is_client_running():
    return CLIENT_PROCESS_NAME in [p.name() for p in psutil.process_iter()]


class Pool(object):

    def __init__(self, worker_class, init_data, password, server_address='localhost', server_port=DEFAULT_PORT):
        self._address = server_address
        self._port = server_port
        self._password = password
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self._address, self._port))

        self._jobs_q, self._results_q, self._reschedule_q = self._init_work(worker_class, init_data)

        # Let the clients connect to the server
        time.sleep(2)

    def imap_unordered(self, data):
        for _, result in self._enumerated_imap_unordered(data):
            yield result

    def map(self, data):

        results = [None] * len(data)

        for index, result in self._enumerated_imap_unordered(data):
            results[index] = result

        return results

    def get_workers_count(self):
        binary = pickle.dumps(('COUNTS_REQUEST', None))
        self._socket.sendall(binary)
        response = self._socket.recv(4096)
        m_type, counts_info = pickle.loads(response)

        assert m_type == 'COUNTS_RESPONSE', 'Invalid response for COUNTS_REQUEST'

        return counts_info['num_workers']

    def update_workers(self, data):
        binary = pickle.dumps(('UPDATE_WORKERS_REQUEST', data))
        self._socket.sendall(binary)

    def close(self):
        self._socket.close()

    def _init_work(self, worker_class, init_data):
        message = {'worker_class': worker_class,  'init_data': init_data}
        binary = pickle.dumps(('WORK_REQUEST', message))
        self._socket.sendall(binary)
        response = self._socket.recv(4096)
        m_type, qs_info = pickle.loads(response)

        assert m_type == 'WORK_ACCEPTED', 'Invalid response for WORK_REQUEST'

        return [RedisQueue(qs_info[q], self._password, self._address) for q in ('jobs_q', 'results_q', 'reschedule_q')]

    def _enumerated_imap_unordered(self, data):

        data = list(data)

        for i, item in enumerate(data):
            self._jobs_q.put(pickle.dumps((i, item)))

        remaining = set(range(len(data)))

        while len(remaining) > 0:

            for message in self._reschedule_q.get_all_nowait():
                reschedule_index = pickle.loads(message)

                if reschedule_index in remaining:
                    print('Dropped job detected. Rescheduling.')
                    self._jobs_q.put(pickle.dumps((reschedule_index, data[reschedule_index])))

            index, result = pickle.loads(self._results_q.get())
            remaining.remove(index)

            yield index, result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class ExperimentWorkerInterface(object):

    def run(self, *args, **kwargs):
        raise NotImplemented()

    def handle_update(self, *args, **kwargs):
        raise NotImplemented