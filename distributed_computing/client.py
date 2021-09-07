import atexit
import os
import signal
import socket
import time
from collections import Counter
from multiprocessing import Process

import GPUtil
import psutil

from distributed_computing.common import RedisQueue
from distributed_computing.globals import MANAGEMENT_Q_NAME


class ExperimentWorkerExecutor(Process):

    def __init__(self, name, client_name, gpu_idx, job_q, results_q, master_q, private_q, worker_class, init_data,
                 master_host, master_port, master_password):

        super(ExperimentWorkerExecutor, self).__init__()

        self.name = name
        self.client_name = client_name
        self.jobs_q_name = job_q
        self.results_q_name = results_q
        self.private_q_name = private_q
        self.master_q_name = master_q
        self.gpu_idx = gpu_idx
        self.init_data = init_data
        self.worker_class = worker_class
        self.head_address = master_host
        self.head_password = master_password
        self.master_port = master_port

    def _get_worker_instance(self, init_data):
        """
        The worker class must be loaded *AFTER* the CUDA_VISIBLE_DEVICES environment variable is set.
        So the module is extracted from the class and re-imported.
        """
        __import__(self.worker_class.__module__, fromlist=[self.worker_class.__name__])
        return self.worker_class(*init_data.get('args', []), **init_data.get('kwargs', {}))

    def run(self):

        jobs_q = RedisQueue(self.jobs_q_name, self.head_password, self.head_address)
        results_q = RedisQueue(self.results_q_name, self.head_password, self.head_address)
        private_q = RedisQueue(self.private_q_name, self.head_password, self.head_address)
        master_q = RedisQueue(self.master_q_name, self.head_password, self.head_address)

        # Without this variable, the GPU numbering is different than the one returned by GPUtil/nvidia-smi
        os.environ['CUDA_DEVICE_ORDER'] = 'PCI_BUS_ID'
        os.environ['CUDA_VISIBLE_DEVICES'] = str(self.gpu_idx)
        worker = self._get_worker_instance(self.init_data)

        while True:

            index, data = jobs_q.get()
            master_q.put(('WORKER_STATUS_UPDATE', {'client': self.client_name, 'worker': self.name,
                                                   'job': index, 'status': 'in_progress'}))
            print(f'\033[96m{self.name} >>\033[0m Processing job {index}')

            for update_data in private_q.get_all_nowait():
                worker.handle_update(*update_data.get('args', []), **update_data.get('kwargs', {}))

            result = worker.run(*data.get('args', []), **data.get('kwargs', {}))
            results_q.put((index, result))
            master_q.put(('WORKER_STATUS_UPDATE', {'client': self.client_name, 'worker': self.name,
                                                   'job': index, 'status': 'finished'}))
            print(f'\033[96m{self.name} >>\033[0m Done processing job {index}')


class PoolClient(object):

    def __init__(self, host, port, password, max_gpus):

        self._host = host
        self._port = port
        self._password = password
        self._max_gpus = max_gpus
        self._workers = {}
        self._gpu_memory_required = 0
        self._name = socket.gethostname()
        self._task_assigned = False
        self._management_q = RedisQueue(MANAGEMENT_Q_NAME, self._password, self._host, self._port)

        atexit.register(self._kill_all_workers)

    def start(self):

        private_q = RedisQueue(self._name, self._password, self._host, self._port)
        self._send_heartbeat_to_server(first=True)

        while True:

            item = private_q.get(timeout=10)

            self._send_heartbeat_to_server()

            # if self._task_assigned:
            #     self._start_available_workers()

            if item is None:
                continue

            message, data = item

            if message == 'TASK_ASSIGNMENT':
                self._handle_task_assignment(data)

            elif message == 'WORKER_RESET':
                self._handle_worker_reset(data)

            else:
                print('Ignoring unknown message:', message)

    def _start_available_workers(self):

        for worker_name, info in self._get_available_gpus().items():
            if worker_name in self._workers:
                continue

            if self._max_gpus is None or len(self._workers) < self._max_gpus:
                if info['free_gpu_memory'] > self._gpu_memory_required:
                    self._workers[worker_name] = info
                    self._workers[worker_name]['pid'] = self._start_worker(worker_name, info['gpu_index'])

    def _handle_worker_reset(self, worker_name):

        self._workers[worker_name]['resets_count'] = self._workers[worker_name].get('resets_count', 0) + 1

        if not psutil.pid_exists(self._workers[worker_name]['pid']):
            print('Worker is already dead. Reset aborted.')
            self._workers.pop(worker_name)
            return

        print('Killing worker:', worker_name)
        self._kill_worker(worker_name)

        if self._workers[worker_name]['resets_count'] == 2:
            self._workers.pop(worker_name)
            print('Two reset requests for worker', worker_name, 'were counted. Stopping it permanently.')
            return

        time.sleep(10)

        print('Restarting worker:', worker_name)
        self._workers[worker_name]['pid'] = self._start_worker(worker_name, self._workers[worker_name]['gpu_index'])

    def _get_available_gpus(self):
        return {str(gpu.id): {'gpu_index': gpu.id,
                              'free_gpu_memory': gpu.memoryFree} for gpu in GPUtil.getGPUs()}

    def _send_heartbeat_to_server(self, first=False):
        self._management_q.put(('CLIENT_HEARTBEAT', {'name': self._name, 'first': first,
                                                     'worker_names': list(self._workers)}))

    def _kill_all_workers(self):
        for worker in self._workers:
            self._kill_worker(worker)

        self._workers = {}

    def _kill_worker(self, worker_name):
        if worker_name in self._workers:
            psutil.Process(self._workers[worker_name]['pid']).send_signal(signal.SIGKILL)

    def _start_worker(self, worker_name, gpu_index):

        worker_q_name = f'{self._name}@{worker_name}'

        worker_process = ExperimentWorkerExecutor(worker_name, self._name, gpu_index, self._jobs_q_name,
                                                  self._results_q_name, MANAGEMENT_Q_NAME, worker_q_name,
                                                  self._worker_class, self._init_data, self._host, self._port,
                                                  self._password)
        worker_process.start()
        return worker_process.pid

    def _handle_task_assignment(self, data):

        self._kill_all_workers()

        self._jobs_q_name = data['jobs_q_name']
        self._results_q_name = data['results_q_name']
        self._worker_class = data['worker_class']
        self._init_data = data['init_data']
        self._gpu_memory_required = data['gpu_memory_required']
        self._task_assigned = True

        self._start_available_workers()

    def stop(self):
        self._kill_all_workers()

