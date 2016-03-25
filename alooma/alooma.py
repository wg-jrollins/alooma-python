import json

import requests

import submodules

EVENT_DROPPING_TRANSFORM_CODE = "def transform(event):\n\treturn None"

DEFAULT_ENCODING = 'utf-8'


class Alooma(object):
    """
    A Python implementation wrapping the Alooma REST API. This API
    provides utility functions allowing a user to perform any action
    the Alooma UI permits, and more.
    """
    def __init__(self, hostname, username, password, port=8443,
                 url_prefix='', eager=False):
        """
        Initializes the Alooma Python API
        :param hostname:   The server to connect to. Typically will be of the
                           form "<your-company-name>.alooma.io"
        :param username:   Your Alooma username
        :param password:   The password associated with your username
        :param port:       (Optional) The destination port, default is 8443
        :param url_prefix: (Optional) A prefix to append to the REST URL
        :param eager:      (Optional) If True, attempts to log in eagerly
        """
        self._hostname = hostname
        self._rest_url = 'https://%s:%d%s/rest/' % (hostname,
                                                    port,
                                                    url_prefix)
        self._username = username
        self._password = password
        self._requests_params = None
        self._cookie = None

        if eager:
            self.__login()

        self._load_submodules()

    def _load_submodules(self):
        """
        Loads all submodules registered in the submodules package.
        Submodules are automatically registered by being put in the
        submodules subfolder. They must contain a 'SUBMODULE_CLASS'
        member pointing to the actual submodule class
        """
        for module_name in submodules.SUBMODULES:
            try:
                submodule = getattr(submodules, module_name)
                submodule_class = getattr(submodule, 'SUBMODULE_CLASS')
                setattr(self, module_name, submodule_class(self))
            except Exception as ex:
                print 'The submodule "%s" could not be loaded. ' \
                      'Exception: %s' % (module_name, ex)

    def _send_request(self, func, url, is_recheck=False, **kwargs):
        """
        Wraps REST requests to Alooma. This function ensures we are logged in
         and that all params exist, and catches any exceptions.
        :param func: a method from the requests package, i.e. requests.get()
        :param url: The destination URL for the REST request
        :param is_recheck: If this is a second try after losing a login
        :param kwargs: Additional arguments to pass to the wrapped function
        :return: The requests.model.Response object returned by the wrapped
        function
        """
        if not self._cookie:
            self.__login()
        if not self._cookie:
            raise Exception('Failed to obtain cookie')

        params = self._requests_params.copy()
        params.update(kwargs)
        response = func(url, **params)

        if self._response_is_ok(response):
            return response

        if response.status_code == 401 and not is_recheck:
            self.__login()

            return self._send_request(func, url, True, **kwargs)

        raise Exception("The rest call to {url} failed: {error_message}".format(
                url=response.url, error_message=response.reason))

    def __login(self):
        """
        Logs into the Alooma server associated with this API instance
        """
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

    @staticmethod
    def _response_is_ok(response):
        return 200 <= response.status_code < 300

    @staticmethod
    def _parse_response_to_json(response):
        return json.loads(response.content.decode(DEFAULT_ENCODING))
