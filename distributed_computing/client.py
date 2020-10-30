import argparse
import os
import pickle
import signal
import sys
import time
from multiprocessing import Process

import GPUtil
import psutil
import setproctitle
from twisted.internet import reactor
from twisted.internet.protocol import ClientFactory

from distributed_computing.globals import CLIENT_PROCESS_NAME
from distributed_computing.common import RedisQueue, BaseProtocol


class ExperimentWorkerExecutor(Process):

    def __init__(self, client_name, gpu_idx, job_q, results_q, private_q, status_q, worker_class, init_data,
                 head_address, head_password):
        super(ExperimentWorkerExecutor, self).__init__()

        self.client_name = client_name
        self.jobs_q_name = job_q
        self.results_q_name = results_q
        self.private_q_name = private_q
        self.status_q_name = status_q
        self.gpu_idx = gpu_idx
        self.init_data = init_data
        self.worker_class = worker_class
        self.head_address = head_address
        self.head_password = head_password

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
        status_q = RedisQueue(self.status_q_name, self.head_password, self.head_address)

        os.environ['CUDA_VISIBLE_DEVICES'] = str(self.gpu_idx)
        worker = self._get_worker_instance(self.init_data)

        while True:

            index, data = pickle.loads(jobs_q.get())
            status_q.put(pickle.dumps((self.client_name, self.gpu_idx, index, 'in_progress')))

            for message in private_q.get_all_nowait():
                update_data = pickle.loads(message)
                worker.handle_update(*update_data.get('args', []), **update_data.get('kwargs', {}))

            result = worker.run(*data.get('args', []), **data.get('kwargs', {}))
            results_q.put(pickle.dumps((index, result)))
            status_q.put(pickle.dumps((self.client_name, self.gpu_idx, index, 'finished')))


class ClientProtocol(BaseProtocol):

    def __init__(self, factory):
        super(ClientProtocol, self).__init__(factory)
        self.factory = factory
        self.factory.worker_processes = []

    def connectionMade(self):
        self.factory.worker_processes = []
        print('Connection succeed. Asking to start workers.')
        self._send_join_request()

    def dataReceived(self, bin_data):

        try:
            message, data = pickle.loads(bin_data)
        except pickle.UnpicklingError as e:
            print('Unpickling failed. Request ignored.')
            return

        if message == 'WAIT_FOR_WORK':
            time.sleep(2)
            self._send_join_request()

        elif message == 'CREATE_WORKERS':
            self._handle_create_workers(data)

    def _handle_create_workers(self, data):

        print(f'Creating {len(data["private_qs"])} worker{"s" if len(data["private_qs"]) > 1 else ""}.')

        self.factory.worker_process = []

        for i, private_q in enumerate(data['private_qs']):

            worker_process = ExperimentWorkerExecutor(data['client_name'], i, data['jobs_q'], data['results_q'],
                                                      private_q, data['status_q'], data['worker_class'],
                                                      data['init_data'], self.factory.head_address,
                                                      self.factory.redis_password)
            worker_process.start()

            self.factory.worker_processes.append(worker_process.pid)

    def _send_join_request(self):
        message = {'num_workers': self.factory.num_gpus}
        self._send_message('JOIN_REQUEST', message)


class WorkerClientFactory(ClientFactory):
    protocol = ClientProtocol

    def __init__(self, num_gpus, head_address, redis_password):
        super(WorkerClientFactory, self).__init__()
        self.num_gpus = num_gpus
        self.head_address = head_address
        self.redis_password = redis_password

    def clientConnectionFailed(self, connector, reason):
        print('Connection failed:', reason.getErrorMessage())
        time.sleep(3)
        print('Trying to reconnect')
        connector.connect()

    def clientConnectionLost(self, connector, reason):
        print('Connection lost:', reason.getErrorMessage())

        print('Terminating worker processes')
        for pid in self.worker_processes:
            psutil.Process(pid).send_signal(signal.SIGKILL)

        time.sleep(3)
        print('Trying to reconnect')
        connector.connect()

    def buildProtocol(self, addr):
        return ClientProtocol(self)


def run_pool_client(address, port, password, violent_exit=False):

    setproctitle.setproctitle(CLIENT_PROCESS_NAME)

    num_gpus = len(GPUtil.getAvailable(maxMemory=0.3, limit=100))
    print(f'Workers client started. {num_gpus} available GPU{"s" if num_gpus > 1 else ""} detected.')

    print(f'Connecting to the pool server on {address}:{port}.')
    factory = WorkerClientFactory(num_gpus, address, password)
    reactor.connectTCP(address, port, factory)

    if violent_exit:
        def force_exit(*args):
            reactor.callFromThread(reactor.stop) # to
            sys.exit(1)

        signal.signal(signal.SIGINT, force_exit)

    reactor.run()
