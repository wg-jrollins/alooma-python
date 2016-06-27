import json

import requests

DEFAULT_TRANSFORM_CODE = "def transform(event):\n\treturn event"


class _CodeEngine(object):
    def __init__(self, api):
        self.api = api

    def get_samples_status_codes(self):
        """
        :return:    a list of status codes each event in Alooma may be tagged
                    with. As Alooma supports more processing capabilities,
                    status codes may be added. These status codes are used for
                    sampling events according to the events' type & status.
        """
        url = self.api._rest_url + 'status-types'
        res = self.api._send_request(requests.get, url)
        return json.loads(res.content)
    
    def get_samples_stats(self):
        """
        :return:    a dictionary where the keys are names of event types,
                    and each value is another dictionary which maps from status
                    code to the amount of samples for that event type & status
        """
        url = self.api._rest_url + 'samples/stats'
        res = self.api._send_request(requests.get, url)
        return json.loads(res.content.decode())
    
    def get_samples(self, event_type=None, error_codes=None):
        """
        :param event_type:  optional string containing an event type name
        :param error_codes: optional list of strings containing event status
                            codes. status codes may be any string returned by
                            `get_sample_status_codes()`
        :return:    a list of 10 samples. if event_type is passed, only samples
                    of that event type will be returned. if error_codes is given
                    only samples of those status codes are returned.
        """
        url = self.api._rest_url + 'samples'
        if event_type:
            url += '?eventType=%s' % event_type
        if error_codes and isinstance(error_codes, list):
            url += ''.join(['&status=%s' % ec for ec in error_codes])
        res = self.api._send_request(requests.get, url)
        return json.loads(res.content)
    
    def get_code(self):
        """
        Returns the currently deployed code from the Alooma Code Engine
        :return: A str representation of the deployed code
        """
        url = self.api._rest_url + 'transform/functions/main'
        try:
            res = self.api._send_request(requests.get, url)
            return self.api._parse_response_to_json(res)["code"]
        except:
            defaults_url = self.api._rest_url + 'transform/defaults'
            res = self.api._send_request(requests.get, defaults_url)
            return self.api._parse_response_to_json(res)["PYTHON"]
    
    def deploy_code(self, code):
        """
        Deploys the submitted code into the Alooma Code Engine. Once
        deployed, the code will be applied to every event flowing
        through the system
        :param code: A string representing valid Python code
        :return: a requests.model.Response object representing the
        result of the REST call
        """
        data = {'language': 'PYTHON', 'code': code,
                'functionName': 'main'}
        url = self.api._rest_url + 'transform/functions/main'
        res = self.api._send_request(requests.post, url, json=data)
        return res

    def deploy_default_code(self):
        """
        Deploys the default code to the Code Engine, which makes it return
        events without transforming them.
        """
        code = DEFAULT_TRANSFORM_CODE
        return self.deploy_code(code=code)
    
    def test_transform(self, sample, temp_transform=None):
        """
        :param sample:  a json string or a dict, representing a sample event
        :param temp_transform: optional string containing transform code. if
                        not provided, the currently deployed transform will be
                        used.
        :return:        the results of a test run of the temp_transform on the
                        given sample. This returns a dictionary with the
                        following keys:
                            'output' - strings printed by the transform function
                            'result' - the resulting event
                            'runtime' - millis it took the function to run
        """
        url = self.api._rest_url + 'transform/functions/run'
        if temp_transform is None:
            temp_transform = self.get_code()
        if not isinstance(sample, dict):
            sample = json.loads(sample)
        data = {
            'language': 'PYTHON',
            'functionName': 'test',
            'code': temp_transform,
            'sample': sample
        }
        res = self.api._send_request(requests.post, url, json=data)
        return json.loads(res.content)
    
    def test_transform_all_samples(self, event_type=None, status_code=None):
        """
        test many samples on the current transform at once
        :param event_type:  optional string containing event type name
        :param status_code: optional status code string
        :return:    a list of samples (filtered by the event type & status code
                    if provided), for each sample, a 'result' key is added which
                    includes the result of the current transform function after
                    it was run with the sample.
        """
        curr_transform = self.get_code()
        samples_stats = self.get_samples_stats()
        results = []
        event_types = [event_type] if event_type else samples_stats.keys()
        for event_type in event_types:
            status_codes = [status_code] if status_code \
                else samples_stats[event_type].keys()
            for sc in status_codes:
                if samples_stats[event_type][sc] > 0:
                    samples = self.get_samples(event_type, sc)
                    for s in samples:
                        s['result'] = self.test_transform(s['sample'],
                                                          curr_transform)
                        results.append(s)
        return results

SUBMODULE_CLASS = _CodeEngine
