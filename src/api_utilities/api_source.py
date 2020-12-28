from .abstract_classes import AbstractSource
import requests
from .constants import *
import json
import urllib

class ApiSource(AbstractSource):
    def __init__(self, method, method_params = {}, other_params = {}):
        self.method = method
        self.method_params = method_params
        self.other_params = other_params

    def set_params(self, mp, op):
        self.method_params = mp
        self.other_params = op

    def get_request_data(self):
        data = {}
        data.update(self.method_params)
        data.update(self.other_params)
        data['method'] = self.method
        data['api_key'] = KEY
        data['format'] = 'json'
        return data
    # read  source
    def read(self):
        try:
            r = requests.post(BASE, data=self.get_request_data())
            return r.json()
        except Exception:
            return {'error' : r, 
                    'method': self.method_params, 
                    'params' : self.method_params, 
                    'other_params' : self.other_params}

if __name__ == "__main__":
    from pprint import pprint
    a = ApiSource(method='track.getinfo', method_params={'artist' : 'cher', 'track' : 'believe'}, other_params={'limit':500})
    pprint(a.read())