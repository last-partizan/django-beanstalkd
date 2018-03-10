import logging
from collections import OrderedDict
from time import sleep
import sys
import os
import signal
import importlib

import beanstalkc

from django.conf import settings
from django.core.management.base import BaseCommand
from django.apps import apps

from django_beanstalkd import BeanstalkClient, BeanstalkError

JOB_NAME = getattr(settings, 'BEANSTALK_JOB_NAME', '%(app)s.%(job)s')
JOB_FAILED_RETRY = getattr(settings, 'BEANSTALK_JOB_FAILED_RETRY', 3)
JOB_FAILED_RETRY_AFTER = getattr(settings,
                                 'BEANSTALK_JOB_FAILED_RETRY_AFTER', 60)
DISCONNECTED_RETRY_AFTER = getattr(
    settings, 'BEANSTALK_DISCONNECTED_RETRY_AFTER', 30)
RESERVE_TIMEOUT = getattr(settings, "BEANSTALK_RESERVE_TIMEOUT", None)
SOCKET_TIMEOUT = getattr(settings, "BEANSTALK_SOCKET_TIMEOUT", 300)

logger = logging.getLogger('django_beanstalkd')
_stream = logging.StreamHandler()
_stream.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s: %(message)s'))
logger.addHandler(_stream)


class Command(BaseCommand):
    help = "Start a Beanstalk worker serving all registered Beanstalk jobs"
    __doc__ = help
    can_import_settings = True
    requires_model_validation = True
    children = []  # list of worker processes
    jobs = OrderedDict()

    def add_arguments(self, parser):
        parser.add_argument(
            '-w', '--workers', dest='worker_count', type=int,
            default=1, help='Number of workers to spawn.'
        )
        parser.add_argument(
            '-l', '--log-level', dest='log_level', type=str,
            default='info', help=(
                'Log level of worker process (one of '
                '"debug", "info", "warning", "error")'
            )
        )
        parser.add_argument(
            '-m', '--module', action='store', dest='module', type=str,
            default='', help='Module to load beanstalk_jobs from'
        )

    def handle(self, *args, **options):  # noqa: C901
        # set log level
        logger.setLevel(getattr(logging, options['log_level'].upper()))

        # find beanstalk job modules
        bs_modules = []
        if not options['module']:
            for app in apps.app_configs.values():
                try:
                    bs_modules.append(importlib.import_module("%s.beanstalk_jobs" % app.name))
                except ImportError as e:
                    if e.message != 'No module named beanstalk_jobs':
                        logger.error(e)
        else:
            bs_modules.append(importlib.import_module("%s.beanstalk_jobs" % options["module"]))
        if not bs_modules:
            logger.error("No beanstalk_jobs modules found!")
            return

        # find all jobs
        jobs = []
        beanstalk_options = {}
        for bs_module in bs_modules:
            try:
                jobs += bs_module.beanstalk_job_list
                beanstalk_options.update(bs_module.beanstalk_jobs.beanstalk_options)
            except AttributeError:
                pass
        if not jobs:
            logger.error("No beanstalk jobs found!")
            return
        self.beanstalk_options = beanstalk_options
        workers = {'default': {}}
        for job in jobs:
            # determine right name to register function with
            app = job.app
            jobname = job.__name__
            func = JOB_NAME % {
                'app': app, 'job': jobname,
            }
            try:
                workers[job.worker][func] = job
            except KeyError:
                workers[job.worker] = {func: job}

        # spawn all workers and register all jobs
        try:
            worker_count = int(options['worker_count'])
            assert(worker_count > 0)
        except (ValueError, AssertionError):
            worker_count = self.get_workers_count('default')

        self.register_sigterm_handler()
        self.spawn_workers(workers, worker_count)

        # start working
        logger.info("Starting to work... (press ^C to exit)")
        try:
            for child in self.children:
                os.waitpid(child, 0)
        except KeyboardInterrupt:
            sys.exit(0)

    def get_workers_count(self, worker):
        return self.beanstalk_options.get('workers', {}).get(worker, 1)

    def register_sigterm_handler(self):
        """Stop child processes after receiving SIGTERM"""
        def handler(sig, func=None):
            for child in self.children:
                os.kill(child, signal.SIGINT)
            sys.exit(0)
        signal.signal(signal.SIGTERM, handler)

    def spawn_workers(self, workers, worker_count):
        """
        Spawn as many workers as desired (at least 1).
        Accepts:
        - workers, {'default': job_list}
        - worker_count, positive int
        """
        # no need for forking if there's only one worker
        job_list = workers.pop('default')
        if worker_count == 1 and not workers:
            return BeanstalkWorker('default', job_list).work()

        # spawn children and make them work (hello, 19th century!)
        def make_worker(name, jobs):
            child = os.fork()
            try:
                # reinit crypto modules after fork
                # http://stackoverflow.com/questions/16981503/pycrypto-assertionerrorpid-check-failed-rng-must-be-re-initialized-after-fo
                import Crypto
                Crypto.Random.atfork()
            except:
                pass
            if child:
                self.children.append(child)
            else:
                BeanstalkWorker(name, jobs).work()
            logger.info(
                "Available jobs (worker '%s'):\n%s",
                name,
                "\n".join(["  * %s" % k for k in jobs.keys()]),
            )
        if job_list:
            for i in range(worker_count):
                make_worker('default', job_list)
        for key, job_list in workers.items():
            for i in range(self.get_workers_count(key)):
                make_worker(key, job_list)
        logger.info("Spawned %d workers", len(self.children))


class BeanstalkWorker(object):
    def __init__(self, name, jobs):
        self.name = name
        self.jobs = jobs

    def work(self):
        """children only: watch tubes for all jobs, start working"""

        self.init_beanstalk()

        logger.info(
            "Available jobs (worker '%s'):\n%s",
            self.name,
            "\n".join(["  * %s" % k for k in self.jobs.keys()]),
        )

        while True:
            try:
                self._worker()
            except KeyboardInterrupt:
                sys.exit(0)
            except beanstalkc.SocketError as e:
                logger.error("disconnected: %s", e, exc_info=True)
                sleep(DISCONNECTED_RETRY_AFTER)
                try:
                    self.init_beanstalk()
                except BeanstalkError as e:
                    logger.error("reconnection failed: %s", e)
                else:
                    logger.debug("reconnected")
            except Exception as e:
                logger.exception(e)

    def init_beanstalk(self):
        self._client = BeanstalkClient()
        self._watch = self._client._beanstalk.watch
        for job in self.jobs.keys():
            self._watch(job)
        self._client._beanstalk.ignore('default')

    def _worker(self):
        job = self._client._beanstalk.reserve()
        stats = job.stats()
        job_name = stats['tube']
        self.process_job(job, job_name, stats)

    def process_job(self, job, job_name, stats):
        job_obj = self.jobs[job_name]
        logger.debug("j:%s, %s(%s)", job.jid, job_name, job.body)
        if RESERVE_TIMEOUT and not job_obj.ignore_reserve_timeout:
            age = stats['age'] - stats['delay']
            if age >= RESERVE_TIMEOUT:
                logger.warning(
                    "job.buried: Job age > RESERVE_TIMEOUT.",
                    extra={
                        'data': {
                            'job': {
                                "tube": job_name,
                                "id": job.jid,
                                "body": job.body,
                                "age": age,
                            },
                        },
                    })
                job.bury()
                return
        try:
            job_obj.call(job)
            logger.debug("j:%s, done->delete", job.jid)
            job.delete()
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.debug(u"%s:%s: job failed (%s)", job.jid, job_name, e)
            if not isinstance(e, job_obj.ignore_exceptions):
                logger.exception(e)
            releases = stats['releases']
            if releases >= JOB_FAILED_RETRY:
                logger.info('j:%s, failed->bury', job.jid)
                try:
                    job_obj.on_bury(job, e)
                except Exception as e:
                    logger.info('j:%s, on_bury failed', job.jid)
                    logger.exception(e)
                job.bury()
                return
            else:
                delay = (releases or 0.1) * JOB_FAILED_RETRY_AFTER
                logger.info('j:%s, failed->retry with delay %ds', job.jid, delay)
                job.release(delay=delay)
