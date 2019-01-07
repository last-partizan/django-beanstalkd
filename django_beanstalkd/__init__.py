"""
Django Beanstalk Interface
"""
from __future__ import absolute_import
import os
import logging

from django.conf import settings

from beanstalkc import Connection, SocketError, DEFAULT_PRIORITY, DEFAULT_TTR

from .decorators import beanstalk_job

__all__ = ['beanstalk_job', 'connect_beanstalkd']


def get_logger(name):
    logger = logging.getLogger('django_beanstalkd')
    if not logger.handlers:
        stream = logging.StreamHandler()
        if "JOURNAL_STREAM" not in os.environ:
            stream.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s: %(message)s'))
        logger.addHandler(stream)
    return logger


def connect_beanstalkd():
    """Connect to beanstalkd server(s) from settings file"""

    server = getattr(settings, 'BEANSTALK_SERVER', '127.0.0.1')
    timeout = getattr(settings, 'BEANSTALK_CONNECT_TIMEOUT', 10)
    port = 11300
    if server.find(':') > -1:
        server, port = server.split(':', 1)

    try:
        port = int(port)
        return Connection(server, port, connect_timeout=timeout)
    except (ValueError, SocketError) as e:
        raise BeanstalkError(server, e)


class BeanstalkError(Exception):
    pass


class BeanstalkClient(object):
    """beanstalk client, automatically connecting to server"""

    def __init__(self, **kwargs):
        self._beanstalk = connect_beanstalkd()

    def call(self, func, arg='', priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR):
        """
        Calls the specified function (in beanstalk terms: put the specified arg
        in tube func)

        priority: an integer number that specifies the priority. Jobs with a
                  smaller priority get executed first
        delay: how many seconds to wait before the job can be reserved
        ttr: how many seconds a worker has to process the job before it gets requeued
        """
        self._beanstalk.use(func)
        return self._beanstalk.put(str(arg), priority=priority, delay=delay, ttr=ttr)

    def peek(self, jid):
        return self._beanstalk.peek(jid)
