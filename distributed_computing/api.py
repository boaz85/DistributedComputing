import socket
import time
from datetime import datetime
from multiprocessing import Process

import psutil
import requests
import setproctitle

from distributed_computing.common import RedisQueue
from distributed_computing.client import PoolClient
from distributed_computing.globals import DEFAULT_PORT, MANAGEMENT_Q_NAME, SERVER_PROCESS_NAME, CLIENT_PROCESS_NAME
from distributed_computing.server import PoolServer


def _run_pool_client(host, port, password, max_gpus):
    setproctitle.setproctitle(CLIENT_PROCESS_NAME)
    client = PoolClient(host, port, password, max_gpus)
    client.start()


def start_node(password, address='localhost', port=DEFAULT_PORT, max_gpus=None):
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

    client = Process(target=_run_pool_client, args=(address, port, password, max_gpus))
    client.start()

    return lambda: client.terminate(), lambda: client.join()


def _run_pool_server(host, port, password):
    setproctitle.setproctitle(SERVER_PROCESS_NAME)
    server = PoolServer(host, port, password)
    server.start()


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

    print('Starting workers pool server.')
    pool_server = Process(target=_run_pool_server, args=('localhost', port, password))
    pool_server.start()

    print('\n' + '\033[36m*\033[0m' * 90)
    print('\033[1mTo start a worker node, type:\033[0m')
    ip = requests.get('https://api.ipify.org').text

    print(f'\033[92mstart_node -p {port} -a {ip} -r {password}\033[0m')
    print('\033[36m*\033[0m' * 90 + '\n')

    return lambda: pool_server.terminate(), lambda: pool_server.join()


def start_head_node(password, port=DEFAULT_PORT, max_gpus=None):
    """
    Start a worker pool server and a worker node on the local machine. A redis server is also started by this function
    if not already running.
    :param password: worker pool password to prevent unauthorized access.
    :param port: workers pool server port.
    :return: a function that terminates both the server and the node process. A function that waits for both it to
    finish.
    """
    server_terminator, server_waiter = start_server(password, port)
    client_terminator, client_waiter = start_node(password, port=port, max_gpus=max_gpus)

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
    def __init__(self, worker_class, init_data, password, server_address='localhost', server_port=DEFAULT_PORT,
                 job_timeout=None, min_gpu_memory_required=11000, min_workers=0):
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

        self._name = f'{socket.gethostname()}@{worker_class.__name__}'

        self._management_q = RedisQueue(MANAGEMENT_Q_NAME, self._password, self._address, self._port)
        self._private_q = RedisQueue(self._name, self._password, self._address, self._port)

        self._management_q.put(('TASK_ASSIGNMENT', {'init_data': init_data, 'worker_class': worker_class,
                                                    'name': self._name, 'job_timeout': job_timeout,
                                                    'min_gpu_memory_required': min_gpu_memory_required}))
        message, data = self._private_q.get()

        if message != 'TASK_APPROVAL':
            raise Exception('Work was not approved by the pool server')

        self._jobs_q_name = data['jobs_q_name']
        self._results_q_name = data['results_q_name']

        # TODO: what to do when workers are leaving and this number is reached?
        if min_workers:
            self.wait_for_workers(min_workers)

    def imap_unordered(self, data):
        """
        Return results once ready. Order is not guaranteed. Similar to multiprocessing.Pool.imap_unordered.
        :param data: List of items to be processed by the worker. This is a dictionary
        of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class run method as args,
        and the kwargs are provided as keyword args.
        """
        for _, result in self._enumerated_imap_unordered(data):
            yield result

    def imap(self, data):
        """
        Return all the results, ordered by the corresponding inputs ordering. Similar to multiprocessing.Pool.map.
        :param data: List of items to be processed by the worker. This is a dictionary
        of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class run method as args,
        and the kwargs are provided as keyword args.
        :return: results list.
        """
        results = {}
        returned = 0

        for index, result in self._enumerated_imap_unordered(data):
            results[index] = result

            if returned in results:
                yield results[returned]
                del results[returned]
                returned += 1

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
        self._management_q.put(('COUNTS_REQUEST', None))
        message, data = self._private_q.get()

        assert message == 'COUNTS_RESPONSE', 'Invalid response for WORKERS_COUNTS'

        return data['workers_count']

    def get_nodes_count(self):
        """
        Get the current number of worker nodes. Not to be confused with the number of worker processes.
        :return: number of worker nodes.
        """
        self._management_q.put(('COUNTS_REQUEST', None))
        message, data = self._private_q.get()

        assert message == 'COUNTS_RESPONSE', 'Invalid response for WORKERS_COUNTS'

        return data['clients_count']

    def wait_for_workers(self, min_workers=1, timeout=None):

        start_time = datetime.now()

        while timeout is None or datetime.now() - start_time < timeout:
            if self.get_workers_count() >= min_workers:
                return

            time.sleep(1)

    def update_workers(self, data):
        """
        Send a synchronous update to all worker processes. The workers will get this update once they finish the
        current job, or immediately if no job is being processed.
        :param data: update data that is passed to the worker handle_update method. This is a dictionary
        of the form {'args': [...], 'kwargs': {...}}. The args are provided to the worker class handle_update method as
        args, and the kwargs are provided as keyword args.
        :return: None
        """
        self._management_q.put(('TASK_UPDATE', data))

    def close(self):
        """
        Close the current pool. Active jobs, if any, are aborted.
        :return: None
        """
        pass

    def _enumerated_imap_unordered(self, jobs_data):

        jobs_q = RedisQueue(self._jobs_q_name, self._password, self._address, self._port)
        results_q = RedisQueue(self._results_q_name, self._password, self._address, self._port)

        jobs_data = list(jobs_data)

        # TODO: postpone the later parts addition if the jobs are too heavy. Push them only when the first ones were
        #  finished.
        for i, item in enumerate(jobs_data):
            jobs_q.put((i, item))

        remaining = set(range(len(jobs_data)))

        while len(remaining) > 0:

            index, result = results_q.get()

            if result is None:
                jobs_q.put((index, jobs_data[index]))
                continue

            if index in remaining:
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