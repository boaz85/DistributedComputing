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
    """
    Start a dedicated redis server on the local machine.
    :param password: worker pool password to prevent unauthorized access.
    :return: None if the redis server is already running, or a process handle if it has been started by this function.
    """

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
    """
    Start a worker node on the local machine.
    :param password: worker pool password to prevent unauthorized access.
    :param address: workers pool server address. If not provided, assumed to be the local machine (localhost).
    :param port: workers pool server port.
    :param violent_exit: Use sys.exit(1) to kill the node when SIGKILL is triggered.
    :return: a function that terminates the node process. A function that waits for it to finish.
    """
    if is_client_running():
        print('A client is already running on this machine.')
        return lambda: 0, lambda: 0

    address = address or 'localhost'

    print('Starting workers pool client.')

    client = Process(target=run_pool_client, args=(address, port, password, violent_exit))
    client.start()

    return lambda: client.terminate(), lambda: client.join()


def start_server(password, port=DEFAULT_PORT):
    """
    Start the worker pool server on the local machine. May be used to separate pool management from worker machines.
    A redis server is also started by this function if not already running.
    :param password: worker pool password to prevent unauthorized access.
    :param port: workers pool server port.
    :return: a function that terminates the pool server process. A function that waits for it to finish.
    """
    if is_server_running():
        print('A server is already running on this machine.')
        return lambda: 0, lambda: 0

    redis_process = start_redis(password)

    print('Starting workers pool server.')
    pool_server = Process(target=run_pool_server, args=(port, password))
    pool_server.start()

    def terminate():
        pool_server.terminate()

        if redis_process is not None:
            redis_process.terminate()

    print('\n' + '\033[36m*\033[0m' * 90)
    print('\033[1mTo start a worker node, type:\033[0m')
    ip = requests.get('https://api.ipify.org').text

    print(f'\033[92mstart_node -p {port} -a {ip} -r {password}\033[0m')
    print('\033[36m*\033[0m' * 90 + '\n')

    return terminate, lambda: pool_server.join()


def start_head_node(password, port=DEFAULT_PORT):
    """
    Start a worker pool server and a worker node on the local machine. A redis server is also started by this function
    if not already running.
    :param password: worker pool password to prevent unauthorized access.
    :param port: workers pool server port.
    :return: a function that terminates both the server and the node process. A function that waits for both it to
    finish.
    """
    server_terminator, server_waiter = start_server(password, port)
    client_terminator, client_waiter = start_node(password, port=port)

    def terminate():
        server_terminator()
        client_terminator()

    def wait():
        server_waiter()
        client_waiter()

    return terminate, wait


def is_server_running():
    """
    Check if a worker pool server is already running on the local machine.
    :return: True if a server is running, else False
    """
    return SERVER_PROCESS_NAME in [p.name() for p in psutil.process_iter()]


def is_client_running():
    """
    Check if a worker node is already running on the local machine.
    :return: True if a server is running, else False
    """
    return CLIENT_PROCESS_NAME in [p.name() for p in psutil.process_iter()]


class Pool(object):
    """
    Worker pool interface, similar to python multiprocessing Pool interface.
    """
    def __init__(self, worker_class, init_data, password, server_address='localhost', server_port=DEFAULT_PORT):
        """
        Initialize pool and connect to the worker pool server.
        :param worker_class: A worker class that implements the WorkerInterface. This class is instantiated on each
        worker process in the worker machines.
        :param init_data: Initialization data that is passed to the worker class constructor. This is a dictionary
        of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class constructor as args,
        and the kwargs are provided as keyword args.
        :param password: worker pool password to prevent unauthorized access.
        :param server_address: workers pool server address. If not provided, assumed to be the local machine
        (localhost).
        :param server_port: workers pool server port.
        """
        self._address = server_address
        self._port = server_port
        self._password = password
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self._address, self._port))

        self._jobs_q, self._results_q, self._reschedule_q = self._init_work(worker_class, init_data)

        # Let the clients connect to the server
        time.sleep(2)

    def imap_unordered(self, data):
        """
        Return results once ready. Order is not guaranteed. Similar to multiprocessing.Pool.imap_unordered.
        :param data: List of items to be processed by the worker. This is a dictionary
        of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class run method as args,
        and the kwargs are provided as keyword args.
        """
        for _, result in self._enumerated_imap_unordered(data):
            yield result

    def map(self, data):
        """
        Return all the results, ordered by the corresponding inputs ordering. Similar to multiprocessing.Pool.map.
        :param data: List of items to be processed by the worker. This is a dictionary
        of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class run method as args,
        and the kwargs are provided as keyword args.
        :return: results list.
        """
        results = [None] * len(data)

        for index, result in self._enumerated_imap_unordered(data):
            results[index] = result

        return results

    def get_workers_count(self):
        """
        Get the current number of worker processes. Not to be confused with the number of worker nodes (machines).
        :return: number of worker processes.
        """
        binary = pickle.dumps(('COUNTS_REQUEST', None))
        self._socket.sendall(binary)
        response = self._socket.recv(4096)
        m_type, counts_info = pickle.loads(response)

        assert m_type == 'COUNTS_RESPONSE', 'Invalid response for COUNTS_REQUEST'

        return counts_info['num_workers']

    def get_nodes_count(self):
        """
        Get the current number of worker nodes. Not to be confused with the number of worker processes.
        :return: number of worker nodes.
        """
        binary = pickle.dumps(('COUNTS_REQUEST', None))
        self._socket.sendall(binary)
        response = self._socket.recv(4096)
        m_type, counts_info = pickle.loads(response)

        assert m_type == 'COUNTS_RESPONSE', 'Invalid response for COUNTS_REQUEST'

        return counts_info['num_clients']

    def update_workers(self, data):
        """
        Send a synchronous update to all worker processes. The workers will get this update once they finish the
        current job, or immediately if no job is being processed.
        :param data: update data that is passed to the worker handle_update method. This is a dictionary
        of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class handle_update method as
        args, and the kwargs are provided as keyword args.
        :return: None
        """
        binary = pickle.dumps(('UPDATE_WORKERS_REQUEST', data))
        self._socket.sendall(binary)

    def close(self):
        """
        Close the current pool. Active jobs, if any, are aborted.
        :return: None
        """
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


class WorkerInterface(object):
    """
    This interface must be implemented by the worker class provided to the Pool object.
    """
    def run(self, *args, **kwargs):
        """
        This method is where the main work is done. It is called for each job, with the corresponding args and kwargs
        provided to the pool.map or pool.imap_unordered functions. The returned value is passed to the caller as is.
        """
        raise NotImplemented()

    def handle_update(self, *args, **kwargs):
        """
        This method is called when the pool.update_workers is called by the pool owner with the provided args and
        kwargs. The returned value is ignored.
        """
        raise NotImplemented