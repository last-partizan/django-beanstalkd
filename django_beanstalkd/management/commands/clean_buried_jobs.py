# coding: utf-8
from __future__ import unicode_literals

from django.core.management.base import BaseCommand

from django_beanstalkd import connect_beanstalkd


class Command(BaseCommand):
    args = ''
    help = 'Remove buried jobs from beantalkd.'

    def handle(self, *args, **options):
        bc = connect_beanstalkd()
        buried_jobs = bc.stats()["current-jobs-buried"]
        for t in bc.tubes():
            bc.use(t)
            while buried_jobs > 0:
                try:
                    j = bc.peek_buried()
                    j.delete()
                    buried_jobs -= 1
                    self.stdout.write("Cleaning buried jobs: %s..." % buried_jobs, ending="\r")
                    self.stdout.flush()
                except AttributeError:
                    break
