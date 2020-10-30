import pickle
import time
import setproctitle

from twisted.internet import protocol, reactor
from twisted.internet.defer import DeferredSemaphore
from twisted.internet.task import LoopingCall

from distributed_computing.globals import SERVER_PROCESS_NAME
from distributed_computing.common import RedisQueue, BaseProtocol


class ServerProtocol(BaseProtocol):

    def __init__(self, *args, **kwargs):
        super(ServerProtocol, self).__init__(*args, **kwargs)

    def connectionLost(self, reason):
        if self == self.factory.head:
            print('Master connection lost. resetting.')
            self._reset_jobs()

        else:
            print('Client connection lost.')
            if self.factory.worker_class is None:
                return

            print('Rescheduling its aborted jobs.')
            self.factory.update_clients_status()

            for worker, jobs in self.factory.status_by_client.get(self.transport.getPeer().host, {}).items():
                for job, status in jobs.items():
                    if status == 'in_progress':
                        self.factory.reschedule_q.put(pickle.dumps(job))

            self.factory.status_by_client[self.transport.getPeer().host] = {}

    def dataReceived(self, bin_data):

        try:
            message, data = pickle.loads(bin_data)
        except pickle.UnpicklingError as e:
            print('Unpickling failed. Request ignored.')
            return

        if message == 'WORK_REQUEST':
            self._handle_work_request(data)

        elif message == 'JOIN_REQUEST':
            self._handle_client_join(data)

        elif message == 'COUNTS_REQUEST':
            self._handle_counts_request()

        elif message == 'UPDATE_WORKERS_REQUEST':
            self._handle_update_workers(data)

    def _handle_client_join(self, info):

        if self.factory.worker_class is None:
            self._send_message('WAIT_FOR_WORK', None)
            return

        self.factory.clients.append(self)
        client_name = self.transport.getPeer().host
        worker_names = []

        for i in range(info['num_workers']):
            worker_name = f'{client_name}@{i}'
            worker_names.append(worker_name)
            q = RedisQueue(worker_name, self.factory.redis_password).clear()
            self.factory.private_queues.append(q)

        message = {'client_name': client_name,
                   'jobs_q': self.factory.jobs_q_name,
                   'results_q': self.factory.results_q_name,
                   'status_q': self.factory.status_q_name,
                   'private_qs': worker_names,
                   'worker_class': self.factory.worker_class,
                   'init_data': self.factory.init_data}

        self._send_message('CREATE_WORKERS', message)

    def _reset_jobs(self):

        self.factory.worker_class = None
        self.factory.status_by_client = {}

        for private_q in self.factory.private_queues:
            private_q.clear()

        # Close existing connections. Clients will react by killing their workers and rejoining.
        for client in self.factory.clients:
            client.transport.loseConnection()

        time.sleep(3)

        self.factory.clients = []
        self.factory.head = None
        self.factory.private_queues = []

    def _handle_work_request(self, info):

        self._reset_jobs()
        self.factory.head = self

        # Initialize new work resources
        self.factory.worker_class = info['worker_class']
        self.factory.init_data = info['init_data']

        self.factory.jobs_q_name = self.factory.worker_class.__name__ + '_jobs'
        self.factory.results_q_name = self.factory.worker_class.__name__ + '_results'
        self.factory.status_q_name = self.factory.worker_class.__name__ + '_status'
        self.factory.reschedule_q_name = self.factory.worker_class.__name__ + '_reschedule'

        self.factory.jobs_q = RedisQueue(self.factory.jobs_q_name, self.factory.redis_password).clear()
        self.factory.results_q = RedisQueue(self.factory.results_q_name, self.factory.redis_password).clear()
        self.factory.status_q = RedisQueue(self.factory.status_q_name, self.factory.redis_password).clear()
        self.factory.reschedule_q = RedisQueue(self.factory.reschedule_q_name, self.factory.redis_password).clear()

        message = {'jobs_q': self.factory.jobs_q_name, 'results_q': self.factory.results_q_name,
                   'reschedule_q': self.factory.reschedule_q_name}

        self._send_message('WORK_ACCEPTED', message)

    def _handle_counts_request(self):

        message = {
            'num_clients': len(self.factory.clients),
            'num_workers': len(self.factory.private_queues),
        }

        self._send_message('COUNTS_RESPONSE', message)

    def _handle_update_workers(self, data):
        for private_q in self.factory.private_queues:
            private_q.put(pickle.dumps(data))


class ServerFactory(protocol.Factory):

    def __init__(self, redis_password):
        self.redis_password = redis_password
        self.clients = []
        self.head = None
        self.private_queues = []
        self.jobs_q = None
        self.results_q = None
        self.status_q = None
        self.worker_class = None
        self.status_by_client = {}

        self.semaphore = DeferredSemaphore(1)
        LoopingCall(self.update_clients_status).start(10, now=False)

    def update_clients_status(self):

        if self.status_q is None:
            return

        self.semaphore.acquire()
        for status_record in self.status_q.get_all_nowait():
            client, worker, job, status = pickle.loads(status_record)
            self.status_by_client.setdefault(client, {}).setdefault(worker, {}).update({job: status})
        self.semaphore.release()

    def buildProtocol(self, addr):
        return ServerProtocol(self)


def run_pool_server(port, password):
    setproctitle.setproctitle(SERVER_PROCESS_NAME)
    factory = ServerFactory(password)
    reactor.listenTCP(port, factory)
    reactor.run()