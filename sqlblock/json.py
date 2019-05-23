# -*- coding: utf-8 -*-

import json
import datetime

from decimal import Decimal
from dataclasses import is_dataclass, asdict as dataclass_asdict


def json_loads(s):
	"""Deserialize s to a python object"""
	return json.loads(s)


def json_dumps(obj):
	return json.dumps(obj, cls=EnhancedJSONEncoder)


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, obj):  # pylint: disable=E0202
        if is_dataclass(obj):
            return dataclass_asdict(obj)

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        
        if isinstance(obj, datetime.timedelta):
            return (datetime.datetime.min + obj).time().isoformat()
        
        if isinstance(obj, (Decimal)) :
            return float(obj)
        
        if isinstance(obj, complex):
            return [obj.real, obj.imag]            
        
        if hasattr(obj, '__json_object__'):
            return obj.__json_object__()
        
        return json.JSONEncoder.default(self, obj)
