from abstract_classes import AbstractSource
import requests
from constants import *
import json

class ApiSource(AbstractSource):
    def __init__(self, method, method_params = {}, other_params = {}):
        self.method = method
        self.method_params = method_params
        self.other_params = other_params

    def __get_request_query(self):
        req_query = f"{BASE}method={self.method}"
        for k, v in self.method_params.items():
            req_query += f'&{k}={v}'
        for k, v in self.other_params.items():
            req_query += f'&{k}={v}'

        req_query += f"&api_key={KEY}&format=json"
        return req_query

    def set_params(self, mp, op):
        self.method_params = mp
        self.other_params = op

    # read  source
    def read(self):
        r = requests.get(self.__get_request_query())
        try:
            return r.json()
        except Exception:
            return {'error' : r, 
                    'method': self.method_params, 
                    'params' : self.method_params, 
                    'other_params' : self.other_params}

if __name__ == "__main__":
    from pprint import pprint
    a = ApiSource(method='user.getrecenttracks', method_params={'user' : 'giacomo109', 'from' : '1608398626'}, other_params={'limit':500})
    pprint(a.read()['recenttracks']['@attr'])