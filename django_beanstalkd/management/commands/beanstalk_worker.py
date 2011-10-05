import logging
from optparse import make_option
import os
import sys
import traceback

from django.conf import settings
from django.core.management.base import NoArgsCommand
from django_beanstalkd import connect_beanstalkd

BEANSTALK_JOB_NAME = getattr(settings, 'BEANSTALK_JOB_NAME', '%(app)s.%(job)s')
BEANSTALK_JOB_FAILED_RETRY = getattr(settings, 'BEANSTALK_JOB_FAILED_RETRY', 3)

logger = logging.getLogger('django_beanstalkd')
logger.addHandler(logging.StreamHandler())

class Command(NoArgsCommand):
    help = "Start a Beanstalk worker serving all registered Beanstalk jobs"
    __doc__ = help
    option_list = NoArgsCommand.option_list + (
        make_option('-w', '--workers', action='store', dest='worker_count',
                    default='1', help='Number of workers to spawn.'),
        make_option('-l', '--log-level', action='store', dest='log_level',
                    default='info', help='Log level of worker process (one of '
                    '"debug", "info", "warning", "error")'),
    )
    children = [] # list of worker processes
    jobs = {}

    def handle_noargs(self, **options):
        # set log level
        logger.setLevel(getattr(logging, options['log_level'].upper()))

        # find beanstalk job modules
        bs_modules = []
        for app in settings.INSTALLED_APPS:
            try:
                bs_modules.append(__import__("%s.beanstalk_jobs" % app))
            except ImportError:
                pass
        if not bs_modules:
            logger.error("No beanstalk_jobs modules found!")
            return

        # find all jobs
        jobs = []
        for bs_module in bs_modules:
            try:
                jobs += bs_module.beanstalk_job_list
            except AttributeError:
                pass
        if not jobs:
            logger.error("No beanstalk jobs found!")
            return
        logger.info("Available jobs:")
        for job in jobs:
            # determine right name to register function with
            app = job.app
            jobname = job.__name__
            func = BEANSTALK_JOB_NAME % {
                    'app': app, 'job': jobname}
            self.jobs[func] = job
            logger.info("* %s" % func)

        # spawn all workers and register all jobs
        try:
            worker_count = int(options['worker_count'])
            assert(worker_count > 0)
        except (ValueError, AssertionError):
            worker_count = 1
        self.spawn_workers(worker_count)

        # start working
        logger.info("Starting to work... (press ^C to exit)")
        try:
            for child in self.children:
                os.waitpid(child, 0)
        except KeyboardInterrupt:
            sys.exit(0)

    def spawn_workers(self, worker_count):
        """
        Spawn as many workers as desired (at least 1).
        Accepts:
        - worker_count, positive int
        """
        # no need for forking if there's only one worker
        if worker_count == 1:
            return self.work()

        logger.info("Spawning %s worker(s)" % worker_count)
        # spawn children and make them work (hello, 19th century!)
        for i in range(worker_count):
            child = os.fork()
            if child:
                self.children.append(child)
                continue
            else:
                self.work()
                break

    def work(self):
        """children only: watch tubes for all jobs, start working"""
        self._beanstalk = connect_beanstalkd()
        for job in self.jobs.keys():
            self._beanstalk.watch(job)
        self._beanstalk.ignore('default')
        
        try:
            while True:
                self._worker()
        except KeyboardInterrupt:
            sys.exit(0)

    def _worker(self):
        while True:
            job = self._beanstalk.reserve()
            job_name = job.stats()['tube']
            if job_name in self.jobs:
                logger.debug("Calling %s with arg: %s" % (job_name, job.body))
                try:
                    self.jobs[job_name](job.body)
                except Exception, e:
                    tp, value, tb = sys.exc_info()
                    logger.error('Error while calling "%s" with arg "%s": '
                        '%s' % (job_name, job.body, e)
                    )
                    logger.debug("%s:%s" % (tp.__name__, value))
                    logger.debug("\n".join(traceback.format_tb(tb)))
                    releases = job.stats()['releases']
                    if releases > BEANSTALK_JOB_FAILED_RETRY:
                        logger.info('Burying job')
                        job.bury()
                        break
                    else:
                        delay = releases * 60
                        logger.info('Releasing job with delay %ds' % delay)
                        job.release(delay=delay)
                else:
                    job.delete()
                    break
            else:
                job.release()
                break
