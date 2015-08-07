import json
from functools import partial

from django.db.models import DateTimeField

_parse_dt = DateTimeField().to_python

def parse_datetime(obj):
    for k, v in obj.items():
        try:
            obj[k] = _parse_dt(v)
        except:
            pass
    return obj

loads = partial(json.loads, object_hook=parse_datetime)
