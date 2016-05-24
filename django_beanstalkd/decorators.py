import importlib

from django import db

import json_decoder

class _beanstalk_job(object):
    """
    Decorator marking a function inside some_app/beanstalk_jobs.py as a
    beanstalk job
    """
    _on_bury = None

    def __init__(self, f,
                 worker='default', json=False,
                 takes_job=False, require_db=False,
                 ignore_reserve_timeout=False,
                 on_bury=None,
                 ):
        self.f = f
        self.__name__ = f.__name__
        self.worker = worker
        self.json = json
        self.takes_job = takes_job
        self.require_db = require_db
        self.ignore_reserve_timeout = ignore_reserve_timeout
        self._on_bury = on_bury

        # determine app name
        parts = f.__module__.split('.')
        if len(parts) > 1:
            self.app = parts[-2]
        else:
            self.app = ''

        # store function in per-app job list (to be picked up by a worker)
        bs_module = importlib.import_module(f.__module__)
        try:
            if self not in bs_module.beanstalk_job_list:
                bs_module.beanstalk_job_list.append(self)
        except AttributeError:
            bs_module.beanstalk_job_list = [self]

    def get_args(self, job=None):
        args, kwargs = (), {}
        if self.takes_job:
            args += (job,)
        if self.json:
            kwargs = json_decoder.loads(job.body)
        else:
            args += (job.body,)
        return args, kwargs

    def call(self, job):
        if self.require_db:
            db.close_old_connections()
        args, kwargs = self.get_args(job)
        return self.f(*args, **kwargs)

    def on_bury(self, job, exception):
        if self._on_bury:
            args, kwargs = self.get_args(job)
            self._on_bury(exception, *args, **kwargs)

def beanstalk_job(func=None, *args, **kwargs):
    def decorator(func):
        return _beanstalk_job(func, *args, **kwargs)
    return decorator(func) if func else decorator
