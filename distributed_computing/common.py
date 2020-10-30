import pickle
import socket
import time

import redis
from twisted.internet import protocol

# https://github.com/andymccurdy/redis-py/issues/722
from distributed_computing.globals import DEFAULT_PORT


def safe_block(foo):
    def safe_foo(self, *args, **kwargs):

        while True:
            try:
                return foo(self, *args, **kwargs)
            except (socket.timeout, redis.exceptions.TimeoutError):
                self._connect()

    return safe_foo


class RedisQueue(object):
    """Simple Queue with Redis Backend"""
    def __init__(self, name, password, host='localhost', namespace='queue'):

        self._host = host
        self._password = password

        self._connect()
        self.key = '%s:%s' %(namespace, name)

    def _connect(self):
        self.__db = redis.Redis(host=self._host, password=self._password, socket_keepalive=True, socket_timeout=60)

    def reconnect(self):
        self._connect()

    @safe_block
    def clear(self):
        self.__db.delete(self.key)
        return self

    @safe_block
    def qsize(self):
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    @safe_block
    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    @safe_block
    def put(self, item):
        """Put item into the queue."""
        self.__db.rpush(self.key, item)

    @safe_block
    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            item = self.__db.blpop(self.key, timeout=timeout)

            if item:
                item = item[1]
        else:
            item = self.__db.lpop(self.key)

        return item

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)

    def get_all_nowait(self):

        messages = []
        message = self.get(False)

        while message is not None:
            messages.append(message)
            message = self.get(False)

        return messages


class BaseProtocol(protocol.Protocol):

    def __init__(self, factory):
        self.factory = factory

    def _send_message(self, m_type, data):

        binary = pickle.dumps((m_type, data))
        self.transport.write(binary)
