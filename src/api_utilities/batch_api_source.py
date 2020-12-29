from .api_source import ApiSource
from pprint import pprint
import os
from datetime import datetime

class BatchApiSource():
    def __init__(self, method):
        self.api_source = ApiSource(method=method)
        self.method_params_list = []
        
    def update_param_list(self, method_params_list):
        self.method_params_list = method_params_list

    def read(self):
        ret = []
        failed_requests = []
        print('BATCH REQUESTS')
        for i, param in enumerate(self.method_params_list):
            print(f'{i+1} out of {len(self.method_params_list)}')
            self.api_source.set_params(param['method_params'], 
                                    param['other_params'])
            data = self.api_source.read()
            if 'error' not in data:
                ret.append(data)
            else:
                e = data
                e.update(param)
                failed_requests.append(e)
        if failed_requests:
            if not os.path.isdir("log"):
                os.makedirs("log")
            with open("log/batch_api_errors.log", 'a+') as f:
                f.write(f"{datetime.today().strftime('%Y%m%d')}\n")
                f.write("\n".join(failed_requests))
        return ret