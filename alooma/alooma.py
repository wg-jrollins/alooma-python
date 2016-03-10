import json

import requests

import _code_engine
import _mapper
import _metrics
import _notifications
import _configurations
import _restream
import _structure
import _redshift

EVENT_DROPPING_TRANSFORM_CODE = "def transform(event):\n\treturn None"

DEFAULT_ENCODING = 'utf-8'


class Alooma(object):
    def __init__(self, hostname, username, password, port=8443,
                 server_prefix=''):

        self._hostname = hostname
        self._rest_url = 'https://%s:%d%s/rest/' % (hostname,
                                                    port,
                                                    server_prefix)
        self._username = username
        self._password = password
        self._requests_params = None
        self._cookie = None
        self.__login()
        if not self._cookie:
            raise Exception('Failed to obtain cookie')

        self.mapper = _mapper._Mapper(self)
        self.code_engine = _code_engine._CodeEngine(self)
        self.restream = _restream._Restream(self)
        self.redshift = _redshift._Redshift(self)
        self.structure = _structure._Structure(self)
        self.notifications = _notifications._Notifications(self)
        self.metrics = _metrics._Metrics(self)
        self.configurations = _configurations._Configurations(self)

    def __send_request(self, func, url, is_recheck=False, **kwargs):
        params = self._requests_params.copy()
        params.update(kwargs)
        response = func(url, **params)

        if response_is_ok(response):
            return response

        if response.status_code == 401 and not is_recheck:
            self.__login()

            return self.__send_request(func, url, True, **kwargs)

        raise Exception("The rest call to {url} failed: {error_message}".format(
                url=response.url, error_message=response.reason))

    def __login(self):
        url = self._rest_url + 'login'
        login_data = {"email": self._username, "password": self._password}
        response = requests.post(url, json=login_data)
        if response.status_code == 200:
            self._cookie = response.cookies
            self._requests_params = {
                    'timeout': 60,
                    'cookies': self._cookie
            }
        else:
            raise Exception('Failed to login to {} with username: '
                            '{}'.format(self._hostname, self._username))


def response_is_ok(response):
    return 200 <= response.status_code < 300


def parse_response_to_json(response):
    return json.loads(response.content.decode(DEFAULT_ENCODING))


def remove_stats(mapping):
    if 'stats' in mapping:
        del mapping['stats']

    if mapping['fields']:
        for index, field in enumerate(mapping['fields']):
            mapping['fields'][index] = remove_stats(field)
    return mapping
