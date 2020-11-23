import os
import subprocess
import tempfile
import time
from datetime import datetime
import atexit
import redis_server
from redis import Redis

from distributed_computing.common import RedisQueue
from distributed_computing.globals import MANAGEMENT_Q_NAME


class PoolServer(object):

    def __init__(self, host, port, password, keep_updates_history=True):
        self._host = host
        self._port = port
        self._password = password
        self._keep_updates_history = keep_updates_history
        self._updates_history = []
        self._clients = {}
        self._last_heartbeat_by_client = {}
        self._init_data = None
        self._management_q = None
        self._jobs_q_name = None
        self._results_q_name = None
        self._job_timeout = None

    def start(self):

        redis_terminator = self.start_redis(self._port)
        atexit.register(redis_terminator)

        self._management_q = RedisQueue(MANAGEMENT_Q_NAME, self._password, self._host, self._port)

        while True:

            self._check_for_disconnected_clients()

            item = self._management_q.get(timeout=5)

            if item is None:
                continue

            message, data = item

            if message == 'TASK_ASSIGNMENT':
                self._handle_task_assignment(data)

            elif message == 'TASK_UPDATE':
                self._handle_task_update(data)

            elif message == 'WORKER_STATUS_UPDATE':
                self._handle_worker_status_update(data)

            elif message == 'COUNTS_REQUEST':
                self._handle_count_request(data)

            elif message == 'CLIENT_HEARTBEAT':
                self._handle_client_heartbeat(data)

            else:
                print(f'Unknown message received: {message}. Ignoring.')

    def _check_for_disconnected_clients(self):

        for client in self._last_heartbeat_by_client:

            if (datetime.now() - self._last_heartbeat_by_client[client]).total_seconds() > 180:
                print(f'Didn\'t hear from {client} for too long. Rescheduling its tasks.')

                # Reschedule client's active jobs
                for worker, info in self._clients[client]['workers'].items():

                    if info.get('active_job') is not None:
                        print('Rescheduling job', info['active_job'])
                        self._results_q.put((info['active_job'], None))
                        info['active_job'] = None
                        info['job_start_time'] = None

                    info['private_q'].clear()

                # If the the client queue is empty, it might be able to reset its workers.
                if self._clients[client]['private_q'].qsize() == 0:
                    print('Asking it to reset its workers.')
                    self._last_heartbeat_by_client[client] = datetime.now()

                    self._clients[client]['private_q'].put(('TASK_ASSIGNMENT', {
                        'init_data': self._init_data, 'jobs_q_name': self._jobs_q_name,
                        'worker_class': self._worker_class, 'results_q_name': self._results_q_name,
                        'gpu_memory_required': self._min_gpu_memory_required
                    }))

                # The client didn't receive the reset request. Give up.
                else:
                    print('Removing it from the registry')
                    self._last_heartbeat_by_client.pop(client)
                    self._clients.pop(client)

    def _handle_client_heartbeat(self, info):

        if self._init_data is None:
            return

        # A new client
        if info['name'] not in self._clients:
            print('The node', info['name'], 'is joining the pool.')
            self._register_client(info)

        # A rejoining client
        elif info['first'] is True:
            for worker, worker_info in self._clients[info['name']]['workers'].items():
                if worker_info['active_job'] is not None:
                    print('Rescheduling job', worker_info['active_job'], 'of', info['name'])
                    self._results_q.put((worker_info['active_job'], None))
                    worker_info['active_job'] = None
                    worker_info['job_start_time'] = None

            ws = 's' if len(info["workers_info"]) > 1 else ''
            print(f'Rejoining the node {info["name"]} with {len(info["worker_names"])} worker{ws}.')
            self._register_client(info)

        # Just a heartbeat
        else:
            self._last_heartbeat_by_client[info['name']] = datetime.now()

            # Update new workers if there are such
            for worker_name in info['worker_names']:
                if worker_name not in self._clients[info['name']]['workers']:
                    worker_q = RedisQueue(f'{info["name"]}@{worker_name}', self._password, self._host,
                                          self._port).clear()
                    self._clients[info['name']]['workers'][worker_name] = {'active_job': None, 'job_start_time': None,
                                                                           'private_q': worker_q}

            if self._job_timeout is None:
                return

            # Check for stuck workers
            for client_name, client_info in self._clients.items():
                for worker_name, worker_info in client_info['workers'].items():

                    if worker_info['job_start_time'] is None:
                        continue

                    time_since_job_start = datetime.now() - worker_info['job_start_time']

                    if time_since_job_start.total_seconds() > self._job_timeout:
                        print('The job', worker_info['active_job'], ' of worker', worker_name, 'on client',
                              client_name, f'is taking too long ({time_since_job_start.total_seconds()} seconds). '
                              'Rescheduling and resetting worker.')
                        self._results_q.put((worker_info['active_job'], None))
                        worker_info['active_job'] = None
                        worker_info['job_start_time'] = None
                        client_info['private_q'].put(('WORKER_RESET', worker_name))

    def _handle_count_request(self, _):
        #print({c: [w for w in cc['workers']] for c, cc in self._clients.items()})
        workers_count = sum(len(c['workers']) for c in self._clients.values())
        self._master_q.put(('COUNTS_RESPONSE', {'clients_count': len(self._clients), 'workers_count': workers_count}))

    def _handle_worker_status_update(self, data):

        if data['client'] not in self._clients:
            print(f'Received a status update from an unknown node: {data["client"]}. Ignoring.')
            return

        if data['worker'] not in self._clients[data['client']]['workers']:
            private_q = RedisQueue(f'{data["client"]}@{data["worker"]}', self._password, self._host, self._port).clear()
            self._clients[data['client']]['workers'][data['worker']] = {'private_q': private_q, 'active_job': None,
                                                                        'job_start_time': None}
        if data['status'] == 'in_progress':

            # If the worker took a new job without finishing the previous one, reschedule the previous one.
            if self._clients[data['client']]['workers'][data['worker']]['active_job'] is not None:
                print(f'An unfinished job detect for the node {data["client"]}. Rescheduling.')
                self._results_q.put((self._clients[data['client']]['workers'][data['worker']]['active_job'], None))

            self._clients[data['client']]['workers'][data['worker']]['active_job'] = data['job']
            self._clients[data['client']]['workers'][data['worker']]['job_start_time'] = datetime.now()

        # The job is done
        else:
            self._clients[data['client']]['workers'][data['worker']]['active_job'] = None
            self._clients[data['client']]['workers'][data['worker']]['job_start_time'] = None

        self._last_heartbeat_by_client[data['client']] = datetime.now()

    def _handle_task_update(self, update_data):

        if self._keep_updates_history:
            self._updates_history.append(update_data)

        for client_name, client_info in self._clients.items():
            for worker_name, worker_info in client_info['workers'].items():
                worker_info['private_q'].put(update_data)

    def _handle_task_assignment(self, registration_info):

        self._init_data = registration_info['init_data']
        self._job_timeout = registration_info['job_timeout']
        self._min_gpu_memory_required = registration_info['min_gpu_memory_required']
        self._worker_class = registration_info['worker_class']
        self._jobs_q_name = f'{registration_info["worker_class"].__name__}_jobs'
        self._results_q_name = f'{registration_info["worker_class"].__name__}_results'
        self._results_q = RedisQueue(self._results_q_name, self._password, self._host, self._port)

        RedisQueue(self._jobs_q_name, self._password, self._host, self._port).clear()

        self._master_q = RedisQueue(registration_info['name'], self._password, self._host, self._port)
        self._updates_history = []

        for client_name, client_info in self._clients.items():

            worker_q_names = {worker_name: f'{client_name}@{worker_name}' for worker_name in client_info['workers']}

            client_info['private_q'].put(('TASK_ASSIGNMENT', {
                'init_data': self._init_data, 'jobs_q_name': self._jobs_q_name, 'worker_class': self._worker_class,
                'results_q_name': self._results_q_name, 'worker_q_names': worker_q_names
            }))

        for client_name, client_info in self._clients.items():
            client_info['private_q'].clear()
            for worker_name, worker_info in client_info['workers'].items():
                worker_info['private_q'].clear()
                worker_info['active_job'] = None
                worker_info['job_start_time'] = None

        time.sleep(10)

        # Assuming that if there were existing workers, they were all killed by now
        self._results_q.clear()

        self._master_q.put(('TASK_APPROVAL', {'jobs_q_name': self._jobs_q_name,
                                              'results_q_name': self._results_q_name}))

    def _register_client(self, registration_info):

        client_q = RedisQueue(registration_info["name"], self._password, self._host, self._port).clear()
        client_info = {'private_q': client_q, 'workers': {}}

        if len(self._updates_history) > 0:
            wp = 's' if len(registration_info['workers_info']) > 1 else ''
            us = 's' if len(self._updates_history) > 1 else ''
            print(f'Updating {registration_info["name"]} worker{wp} with {len(self._updates_history)} update{us}')

        self._clients[registration_info['name']] = client_info
        self._last_heartbeat_by_client[registration_info["name"]] = datetime.now()

        client_q.put(('TASK_ASSIGNMENT', {
            'init_data': self._init_data, 'jobs_q_name': self._jobs_q_name, 'worker_class': self._worker_class,
            'results_q_name': self._results_q_name, 'gpu_memory_required': self._min_gpu_memory_required
        }))

    def start_redis(self, port):
        """
        Start a dedicated redis server on the local machine.
        :param password: worker pool password to prevent unauthorized access.
        :return: None if the redis server is already running, or a process handle if it has been started by this function.
        """

        try:
            Redis(f'localhost:{port}', password=self._password, socket_connect_timeout=1).ping()
            return lambda: None
        except:
            _, conf_file = tempfile.mkstemp()

            with open(conf_file, 'w') as f:
                f.write(f'requirepass {self._password}')

            FNULL = open(os.devnull, 'w')

            print('Redis server is started.')

            p = subprocess.Popen([redis_server.REDIS_SERVER_PATH, conf_file, '--protected-mode no'], stdout=FNULL)

            return lambda: p.terminate()