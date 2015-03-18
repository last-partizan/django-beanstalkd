import json
from functools import partial

from django.db.models import DateTimeField

class JSONDecoder(json.JSONDecoder):

    def decode(self, *args, **kwargs):
        obj = super(JSONDecoder, self).decode(*args, **kwargs)
        print obj
        return obj

def parse_datetime(obj):
    for k, v in obj.items():
        try:
            obj[k] = DateTimeField().to_python(v)
        except:
            pass
    return obj

loads = partial(json.loads, object_hook=parse_datetime)
