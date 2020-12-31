from datetime import datetime, date
import json
from dataclasses import is_dataclass, asdict
from decimal import Decimal

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if is_dataclass(o):
            return asdict(o)

        if isinstance(o, (date, datetime)):
            return o.isoformat()

        if isinstance(o, Decimal):
            return str(o)

        if isinstance(o, complex):
            return [o.real, o.imag]

        return json.JSONEncoder.default(self, o)

def json_loads(s):
    return json.loads(s)

def json_dumps(obj):
    return json.dumps(obj, cls=JSONEncoder)

def json_object_equals(json_a, json_b):
    if type(json_a) != type(json_b):
        return False

    if type(json_a) == dict:
        if len(json_a) != len(json_b):
            return False
        for key_a in json_a:
            if key_a not in json_b or not json_object_equals(json_a[key_a], json_b[key_a]):
                return False
    
    elif type(json_a) == list:
        if len(json_a) != len(json_b):
            return False
        for itemA, itemB in zip(json_a, json_b):
            if not json_object_equals(itemA, itemB):
                return False
    
    else:
        return json_a == json_b


