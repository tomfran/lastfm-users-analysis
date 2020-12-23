from api_source import ApiSource

class BatchApiSource():
    def __init__(self, method, method_params_list):
        self.api_source = ApiSource(method=method)
        self.method_params_list = method_params_list

    def read(self):
        ret = []
        failed_requests = []
        for param in self.method_params_list:
            self.api_source.set_params(param['method_params'], 
                                       param['other_params'])
            data = self.api_source.read()
            if 'error' not in data:
                ret.append(data)
            else:
                failed_requests.append(data)

        # TODO: manage failed requests
        return ret