import importlib

from django import db

import json_decoder

class _beanstalk_job(object):
    """
    Decorator marking a function inside some_app/beanstalk_jobs.py as a
    beanstalk job
    """

    def __init__(self, f, worker, json=False, takes_job=False, require_db=False, ignore_reserve_timeout=False):
        self.f = f
        self.__name__ = f.__name__
        self.worker = worker
        self.json = json
        self.takes_job = takes_job
        self.require_db = require_db
        self.ignore_reserve_timeout = ignore_reserve_timeout
        
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

    def __call__(self, job):
        args, kwargs = (), {}
        if self.require_db:
            db.close_old_connections()
        if self.takes_job:
            args += (job,)
        if self.json:
            kwargs = json_decoder.loads(job.body)
        else:
            args += (job.body,)
        return self.f(*args, **kwargs)

def beanstalk_job(func=None, worker="default", json=False, takes_job=False, require_db=False):
    def decorator(func):
        return _beanstalk_job(func, worker, json, takes_job, require_db)
    return decorator(func) if func else decorator
