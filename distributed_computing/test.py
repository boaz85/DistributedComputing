import multiprocessing
import time

from distributed_computing.api import start_head_node, WorkerInterface, Pool

PASSWORD = '9976145'


class TestWorker(WorkerInterface):

    def __init__(self, arg, kwarg=None):
        self._arg = arg
        self._kwarg = kwarg
        print('Initialization done')

    def run(self, arg, kwarg=None):
        print('Running for', arg, 'seconds')
        time.sleep(arg)
        print('Done running')
        return kwarg

    def handle_update(self, arg, kwarg=None):
        print('Update received')


if __name__ == '__main__':

    multiprocessing.set_start_method('spawn')

    print('######## Starting head node')
    terminate, wait = start_head_node(PASSWORD)

    print('######## Done starting head node')
    init_data = {'args': [1], 'kwargs': {'kwarg': 2}}

    print('######## Creating pool')
    pool = Pool(TestWorker, init_data, PASSWORD)

    data = [{'args': [i], 'kwargs': {'kwarg': i}} for i in range(30)]

    print('######## Processing data')
    for result in pool.imap_unordered(data):
        print('Result:', result)
        pool.update_workers({'args': [result], 'kwargs': {'kwarg': result}})

    print('######## Terminating')
    terminate()

