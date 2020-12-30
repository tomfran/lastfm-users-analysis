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
        print(f'Batch API requests for {self.api_source.method}')
        for i, param in enumerate(self.method_params_list):
            print(f"\r[ ]\tRequest {i+1} out of {len(self.method_params_list)}", end = '', flush=True)
            self.api_source.set_params(param['method_params'], 
                                    param['other_params'])
            data = self.api_source.read()
            if 'error' not in data:
                ret.append(data)
            else:
                e = data
                e.update(param)
                failed_requests.append(e)

        print("\r[\033[1m\033[92m✓\033[0m]\tAll requests done\033[K")
        
        if failed_requests:
            if not os.path.isdir("log"):
                os.makedirs("log")
            with open("log/batch_api_errors.log", 'a+') as f:
                f.write(f"{datetime.today().strftime('%Y%m%d')}\n")
                f.write(f'{failed_requests}\n')
        return ret