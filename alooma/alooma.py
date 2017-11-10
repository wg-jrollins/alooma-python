import json
import time
import re
import requests
import warnings
from six.moves import urllib

MAPPING_MODES = ['AUTO_MAP', 'STRICT', 'FLEXIBLE']
EVENT_DROPPING_TRANSFORM_CODE = "def transform(event):\n\treturn None"
DEFAULT_TRANSFORM_CODE = "def transform(event):\n\treturn event"

DEFAULT_SETTINGS_EMAIL_NOTIFICATIONS = {
    "digestInfo": True,
    "digestWarning": True,
    "digestError": True,
    "digestFrequency": "DAILY",
    "recipientsChanged": False,
    "recipients": []
}

DEFAULT_ENCODING = 'utf-8'

RESTREAM_QUEUE_TYPE_NAME = "RESTREAM"

OUTPUTS = {
    "redshift": {
        "type": "REDSHIFT",
        "name": "Redshift"
    },
    "snowflake": {
        "type": "SNOWFLAKE",
        "name": "Snowflake"
    },
    "bigquery": {
        "type": "BIGQUERY",
        "name": "BigQuery"
    }
}

METRICS_LIST = [
    'EVENT_SIZE_AVG',
    'EVENT_SIZE_TOTAL',
    'EVENT_PROCESSING_RATE',
    'CPU_USAGE',
    'MEMORY_CONSUMED',
    'MEMORY_LEFT',
    'INCOMING_EVENTS',
    'RESTREAMED_EVENTS',
    'UNMAPPED_EVENTS',
    'IGNORED_EVENTS',
    'ERROR_EVENTS',
    'LOADED_EVENTS_RATE',
    'LATENCY_AVG',
    'LATENCY_PERCENTILE_50',
    'LATENCY_PERCENTILE_95',
    'LATENCY_MAX',
    'EVENTS_IN_PIPELINE',
    'EVENTS_IN_TRANSIT'
]

DEFAULT_TIMEOUT = 60

MAPPING_TIMEOUT = 300

BASE_URL = 'https://app.alooma.com'


class FailedToCreateInputException(Exception):
    pass


class Client(object):
    def __init__(self, username=None, password=None, account_name=None,
                 base_url=None):

        if base_url is None:
            base_url = BASE_URL
        # for backwards compatibility (alooma_dev.py)
        base_url = base_url.rstrip('/')

        rest_path = '/rest/'
        if account_name is not None:
            rest_path = '/%s/rest/' % account_name

        self.rest_url = base_url + rest_path

        self.username = username
        self.password = password
        self.cookie = None
        self.requests_params = {
            'timeout': DEFAULT_TIMEOUT,
            'cookies': self.cookie
        }
        self.account_name = self.__get_account_name()

    def __send_request(self, func, url, is_recheck=False, **kwargs):
        params = self.requests_params.copy()
        params.update(kwargs)
        response = func(url, **params)

        if response_is_ok(response):
            return response

        if response.status_code == 401 and not is_recheck:
            self.__login()

            return self.__send_request(func, url, True, **kwargs)

        raise Exception("The rest call to {url} failed\n"
                        "failure reason: {failure_reason}"
                        "{failure_content}"
                        .format(url=response.url,
                                failure_reason=response.reason,
                                failure_content="\nfailure content: " +
                                                response.content.decode()
                                if response.content.decode() else ""))

    def __login(self):
        url = self.rest_url + 'login'
        login_data = {"email": self.username, "password": self.password}
        response = requests.post(url, json=login_data)
        if response.status_code == 200:
            self.cookie = response.cookies
            self.requests_params['cookies'] = self.cookie
        else:
            raise Exception('Failed logging in with user: {}'
                            .format(self.username))

    def __get_account_name(self):
        url = self.rest_url + 'repository'
        res = self.__send_request(requests.get, url)
        return res.json().get('config_clientName')

    def get_config(self):
        """
        Exports the entire system configuration in dict format.
        This is also used periodically by Alooma for backup purposes,
        :return: a dict representation of the system configuration
        """
        url_get = self.rest_url + 'config/export'
        response = self.__send_request(requests.get, url=url_get)
        config_export = parse_response_to_json(response)
        return config_export

    def get_plumbing(self):
        """
        DEPRECATED - use get_structure() instead.
        Returns a representation of all the inputs, outputs,
        and on-stream processors currently configured in the system
        :return: A dict representing the structure of the system
        """
        return self.get_structure()

    def get_structure(self):
        """
        Returns a representation of all the inputs, outputs,
        and on-stream processors currently configured in the system
        :return: A dict representing the structure of the system
        """
        url_get = self.rest_url + 'plumbing/?resolution=1min'
        response = self.__send_request(requests.get, url_get)
        return parse_response_to_json(response)

    def get_mapping_mode(self):
        """
        Returns the default mapping mode currently set in the system.
        The mapping mode should be one of the values in
        alooma.MAPPING_MODES
        """
        url = self.rest_url + 'mapping-mode'
        res = self.__send_request(requests.get, url)
        return res.content

    def set_mapping_mode(self, mode):
        """
        Sets the default mapping mode in the system. The mapping
        mode should be one of the values in alooma.MAPPING_MODES
        """
        url = self.rest_url + 'mapping-mode'
        res = requests.post(url, json=mode, **self.requests_params)
        return res

    def get_event_types(self):
        """
        Returns a dict representation of all the event-types which
        exist in the system
        """
        url = self.rest_url + 'event-types'
        res = self.__send_request(requests.get, url)
        return parse_response_to_json(res)

    def get_event_type(self, event_type):
        """
        Returns a dict representation of the requested event-type's
        mapping and metadata if it exists
        :param event_type:  The name of the event type
        :return: A dict representation of the event-type's data
        """
        event_type = urllib.parse.quote(event_type, safe='')
        url = self.rest_url + 'event-types/' + event_type

        res = self.__send_request(requests.get, url)
        return parse_response_to_json(res)

    def get_mapping(self, event_type):
        """
        Returns a dict representation of the mapping of the event
        type requested, if it exists
        :param event_type: The name of the event type
        :return: A dict representation of the mapping
        """
        event_type = self.get_event_type(event_type)
        mapping = remove_stats(event_type)
        return mapping

    def get_schemas(self):
        """
        Returns a dict representation of the redshift schema,
        supported only for versions 0.5.15 or uppepr

        :return: A dict representation of the redshift schema
        """
        url = self.rest_url + "schemas/"

        res = self.__send_request(requests.get, url)
        return parse_response_to_json(res)

    def create_s3_input(self, name, key, secret, bucket, prefix='',
                        load_files='all', file_format="json", delimiter=",",
                        quote_char="", escape_char="", one_click=True):
        """
        Creates an S3 input using the supplied configurations
        :param name: The designated input name
        :param key: a valid AWS access key
        :param secret: a valid AWS secret key
        :param bucket: The bucket where the data resides
        :param prefix: An optional file path prefix. If supplied,
        only files in paths matching the prefix will be retrieved
        :param load_files: Can be either 'all' or 'new'. If 'new'
        is selected, only files which are created/updated after the
        input was created will be retrieved. Default is 'all'.
        :param file_format: S3 file format. "json", "delimited", "other".
        :param delimiter: When choosing file format delimited.
        Delimiter character (\t for tab)
        :param quote_char: When choosing file format delimited.
        File quote character (optional)
        :param escape_char: When choosing file format delimited.
        Escape character (optional)
        :return: a requests.model.Response object representing the
        result of the create_input call
        """
        formats = ["json", "delimited", "other"]
        if file_format not in formats:
            raise ValueError("File format cannot be '{file_format}', "
                             "it has to be one of those: {formats}"
                             .format(file_format=file_format,
                                     formats=", ".join(formats)))

        file_format_config = {"type": file_format}
        if file_format == "delimited":
            for key, value in {"delimiter": delimiter,
                               "quoteChar": quote_char,
                               "escapeChar": escape_char}.items():
                if value:
                    file_format_config[key] = value

        post_data = {
            'name': name,
            'type': 'S3',
            'configuration': {
                'awsAccessKeyId': key,
                'awsBucketName': bucket,
                'awsSecretAccessKey': secret,
                'filePrefix': prefix,
                'loadFiles': load_files,
                'fileFormat': json.dumps(file_format_config)
            }
        }
        return self.create_input(input_post_data=post_data,
                                 one_click=one_click)

    def create_mixpanel_input(self, mixpanel_api_key, mixpanel_api_secret,
                              from_date, name, transform_id=None,
                              one_click=True):
        post_data = {
            "name": name,
            "type": "MIXPANEL",
            "configuration": {
                "mixpanelApiKey": mixpanel_api_key,
                "mixpanelApiSecret": mixpanel_api_secret,
                "fromDate": from_date
            }
        }
        return self.create_input(input_post_data=post_data,
                                 one_click=one_click)

    def create_input(self, input_post_data, one_click=True, validate=True):
        structure = self.get_structure()
        previous_nodes = [x for x in structure['nodes']
                          if x['name'] == input_post_data['name']]
        if one_click:
            input_post_data['configuration']['auto_map'] = "true"

        if not validate:
            url = self.rest_url + ('inputs%s' % "?validate=false")
        else:
            url = self.rest_url + 'plumbing/inputs'

        self.__send_request(requests.post, url, json=input_post_data)

        new_id = None
        retries_left = 10
        while retries_left > 0:
            retries_left -= 1
            structure = self.get_structure()
            input_type_nodes = [x for x in structure['nodes'] if x['name'] ==
                                input_post_data["name"]]
            if len(input_type_nodes) == len(previous_nodes) + 1:
                old_ids = set([x['id'] for x in previous_nodes])
                current_ids = set([x['id'] for x in input_type_nodes])
                try:
                    new_id = current_ids.difference(old_ids).pop()
                except KeyError:
                    pass

                return new_id
            time.sleep(1)

        raise FailedToCreateInputException(
            'Failed to create {type} input'.format(
                type=input_post_data["type"]))

    def edit_input(self, input_post_data):
        input_id = input_post_data.get('id')
        if not input_id:
            raise Exception('Could not edit input without id')

        url = self.rest_url + ('inputs/%s' % input_id)
        res = self.__send_request(requests.put, url, json=input_post_data)
        return res

    def create_schema(self, schema_post_data):
        url = self.rest_url + "schemas"

        res = self.__send_request(requests.post, url, json=schema_post_data)
        return res

    def get_transform_node_id(self):
        transform_node = self._get_node_by('type', 'TRANSFORMER')
        if transform_node:
            return transform_node['id']

        raise Exception('Could not locate transform id for %s' %
                        self.account_name)

    def remove_input(self, input_id):
        """
        :param input_id: the id for a given input

        for example:
                This can be found in the url in the input settings
        """
        url = "{rest_url}plumbing/nodes/remove/{input_id}".format(
            rest_url=self.rest_url, input_id=input_id)
        self.__send_request(requests.post, url)

    def set_transform_to_default(self):
        """
        Sets the Code Engine Python code to the default, which makes
        no changes in any event
        """
        transform = DEFAULT_TRANSFORM_CODE
        self.set_transform(transform=transform)

    def set_mapping(self, mapping, event_type, timeout=MAPPING_TIMEOUT):

        event_type = urllib.parse.quote(event_type, safe='')
        url = self.rest_url + 'event-types/{event_type}/mapping'.format(
            event_type=event_type)
        res = self.__send_request(requests.post, url, json=mapping, timeout=timeout)
        return res

    def discard_event_type(self, event_type):
        """
        :param event_type: event name found in the mapper
        """
        event_type_json = {
            "name": event_type,
            "mapping": {
                "isDiscarded": True,
                "tableName": ""
            },
            "fields": [], "mappingMode": "STRICT"
        }
        self.set_mapping(event_type_json, event_type)

    def discard_field(self, mapping, field_path):
        """
        :param mapping: this is the mapping json
        :param field_path:  this would use us to find the keys

        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  the "c" field_path == a.b.c
        :return: new mapping JSON that the last argument would be discarded
        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  field_path == "a.b.c"
                        3.  then the mapping would be as the old but the "c"
                            field that would be discarded
        """

        field = self.find_field_name(mapping, field_path)
        if field:
            if field['mapping'] is None:
                field['mapping'] = {}
            field["mapping"]["isDiscarded"] = True
            field["mapping"]["columnName"] = ""
            field["mapping"]["columnType"] = None

    def unmap_field(self, mapping, field_path):
        """
        :param mapping: this is the mapping json
        :param field_path:  this would use us to find the keys

        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  the "c" field_path == a.b.c
        :return: new mapping JSON that the last argument would be removed
        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  field_path == "a.b.c"
                        3.  then the mapping would be as the old but the "c"
                            field that would be removed -> {"a":1, b:{}}
        """
        field = self.find_field_name(mapping, field_path)
        if field:
            mapping["fields"].remove(field)

    @staticmethod
    def map_field(schema, field_path, column_name, field_type, non_null,
                  **type_attributes):
        """
        :param  schema: this is the mapping json
        :param  field_path: this would use us to find the keys
        :param  field_type: the field type (VARCHAR, INT, FLOAT...)
        :param  type_attributes:    some field type need different attributes,
                                    for example:
                                        1. INT doesn't need any attributes.
                                        2. VARCHAR need the max column length
        :param column_name: self descriptive
        :param non_null: self descriptive
        :return: new mapping dict with new argument
        """

        field = Client.find_field_name(schema, field_path, True)
        Client.set_mapping_for_field(field, column_name, field_type,
                                     non_null, **type_attributes)

    @staticmethod
    def set_mapping_for_field(field, column_name,
                              field_type, non_null, **type_attributes):
        column_type = {"type": field_type, "nonNull": non_null}
        column_type.update(type_attributes)
        field["mapping"] = {
            "columnName": column_name,
            "columnType": column_type,
            "isDiscarded": False
        }

    @staticmethod
    def add_field(parent_field, field_name):
        field = {
            "fieldName": field_name,
            "fields": [],
            "mapping": None
        }
        parent_field["fields"].append(field)
        return field

    @staticmethod
    def find_field_name(schema, field_path, add_if_missing=False):
        """
        :param schema:  this is the dict that this method run over ot
                        recursively
        :param field_path: this would use us to find the keys
        :param add_if_missing: add the field if missing
        :return:    the field that we wanna find and to do on it some changes.
                    if the field is not found then raise exception
        """
        fields_list = field_path.split('.', 1)
        if not fields_list:
            return None

        current_field = fields_list[0]
        remaining_path = fields_list[1:]

        field = next((field for field in schema["fields"]
                      if field['fieldName'] == current_field), None)
        if field:
            if not remaining_path:
                return field
            return Client.find_field_name(field, remaining_path[0])
        elif add_if_missing:
            parent_field = schema
            for field in fields_list:
                parent_field = Client.add_field(parent_field, field)
            return parent_field
        else:
            # raise this if the field is not found,
            # not standing with the case ->
            # field["fieldName"] == field_to_find
            raise Exception("Could not find field path")

    def get_input_sleep_time(self, input_id):
        """
        :param input_id:    ID of the input whose sleep time to return
        :return:            sleep time of the input with ID input_id
        """
        url = self.rest_url + 'inputSleepTime/%s' % input_id
        res = requests.get(url, **self.requests_params)
        return float(json.loads(res.content).get('inputSleepTime'))

    def set_input_sleep_time(self, input_id, sleep_time):
        """
        :param input_id:    ID of the input whose sleep time to change
        :param sleep_time:  new sleep time to set for input with ID input_id
        :return:            result of the REST request
        """
        url = self.rest_url + 'inputSleepTime/%s' % input_id
        res = requests.put(url, json=sleep_time, **self.requests_params)
        return res

    def get_samples_status_codes(self):
        """
        :return:    a list of status codes each event in Alooma may be tagged
                    with. As Alooma supports more processing capabilities,
                    status codes may be added. These status codes are used for
                    sampling events according to the events' type & status.
        """
        url = self.rest_url + 'status-types'
        res = requests.get(url, **self.requests_params)
        return json.loads(res.content)

    def get_samples_stats(self):
        """
        :return:    a dictionary where the keys are names of event types,
                    and each value is another dictionary which maps from status
                    code to the amount of samples for that event type & status
        """
        url = self.rest_url + 'samples/stats'
        res = requests.get(url, **self.requests_params)
        return json.loads(res.content.decode())

    def get_samples(self, event_type=None, error_codes=None):
        """
        :param event_type:  optional string containing an event type name
        :param error_codes: optional list of strings containing event status
                            codes. status codes may be any string returned by
                            `get_sample_status_codes()`
        :return:    a list of 10 samples. if event_type is passed, only samples
                    of that event type will be returned. if error_codes
                    is given only samples of those status codes are returned.
        """
        url = self.rest_url + 'samples'
        if event_type:
            url += '?eventType=%s' % event_type
        if error_codes and isinstance(error_codes, list):
            url += ''.join(['&status=%s' % ec for ec in error_codes])
        res = requests.get(url, **self.requests_params)
        return json.loads(res.content)

    def get_all_transforms(self):
        """
        Returns a map from module name to module code
        """
        url = self.rest_url + 'transform/functions'
        res = self.__send_request(requests.get, url)
        # from list of CodeSnippets to {moduleName: code} mapping
        return {item['functionName']: item['code'] for item in res.json()}

    def get_transform(self, module_name='main'):
        url = self.rest_url + 'transform/functions/{}'.format(module_name)
        try:
            res = self.__send_request(requests.get, url)
            return parse_response_to_json(res)["code"]
        except:
            if module_name == 'main':
                defaults_url = self.rest_url + 'transform/defaults'
                res = self.__send_request(requests.get, defaults_url)
                return parse_response_to_json(res)["PYTHON"]
            else:
                # TODO: remove silent defaults?
                # notify user of lack of code if not main
                raise

    def set_transform(self, transform, module_name='main'):
        data = {'language': 'PYTHON', 'code': transform,
                'functionName': module_name}
        url = self.rest_url + 'transform/functions/{}'.format(module_name)
        res = self.__send_request(requests.post, url, json=data)
        return res

    def test_transform(self, sample, temp_transform=None):
        """
        :param sample:  a json string or a dict, representing a sample event
        :param temp_transform: optional string containing transform code. if
                        not provided, the currently deployed transform will be
                        used.
        :return:        the results of a test run of the temp_transform on the
                        given sample. This returns a dictionary with the
                        following keys:
                            'output' - strings printed by the transform
                                       function
                            'result' - the resulting event
                            'runtime' - millis it took the function to run
        """
        url = self.rest_url + 'transform/functions/run'
        if temp_transform is None:
            temp_transform = self.get_transform()
        if not isinstance(sample, dict):
            sample = json.loads(sample)
        data = {
            'language': 'PYTHON',
            'functionName': 'main',
            'code': temp_transform,
            'sample': sample
        }
        res = requests.post(url, json=data, **self.requests_params)
        return json.loads(res.content)

    def test_transform_all_samples(self, event_type=None, status_code=None):
        """
        test many samples on the current transform at once
        :param event_type:  optional string containing event type name
        :param status_code: optional status code string
        :return:    a list of samples (filtered by the event type & status code
                    if provided), for each sample, a 'result' key is added
                    which includes the result of the current transform function
                    after it was run with the sample.
        """
        curr_transform = self.get_transform()
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

    def get_metrics_by_names(self, metric_names, minutes):
        if type(metric_names) != list and type(metric_names) == str:
            metric_names = [metric_names]
        elif type(metric_names) != str and type(metric_names) != list:
            raise Exception("metric_names can be only from type `str` or "
                            "`list`")
        for metric_name in metric_names:
            if metric_name not in METRICS_LIST:
                raise Exception("Metrics '{name}' not exists, please "
                                "use one or more of those: {metrics}"
                                .format(name=metric_names,
                                        metrics=METRICS_LIST))

        metrics_string = ",".join(metric_names)
        url = self.rest_url + 'metrics?metrics=%s&from=-%dmin' \
                              '&resolution=%dmin' \
                              '' % (metrics_string, minutes, minutes)
        res = self.__send_request(requests.get, url)

        response = parse_response_to_json(res)
        return response

    def get_incoming_queue_metric(self, minutes):
        response = self.get_metrics_by_names("EVENTS_IN_PIPELINE", minutes)
        incoming = non_empty_datapoint_values(response)
        if incoming:
            return max(incoming)
        else:
            return 0

    def get_outputs_metrics(self, minutes):
        """
        Returns the number of events erred / unmapped / discarded / loaded in
        the last X minutes
        :param minutes - number of minutes to check
        """
        response = self.get_metrics_by_names(['UNMAPPED_EVENTS',
                                              'IGNORED_EVENTS',
                                              'ERROR_EVENTS',
                                              'LOADED_EVENTS_RATE'],
                                             minutes)
        return tuple([sum(non_empty_datapoint_values([r])) for r in response])

    def get_restream_queue_metrics(self, minutes):
        response = self.get_metrics_by_names("EVENTS_IN_TRANSIT", minutes)
        return non_empty_datapoint_values(response)[0]

    def get_restream_stats(self):
        """
        Get restream stats;
        - number of available events to restream
        - Restream used size in bytes
        - Max restream size in bytes
        :return: :type dict with the following keys; number_of_events,
                       size_used, max_size
        """
        restream_stats = next(node["stats"]
                              for node in self.get_structure()["nodes"]
                              if node["type"] == RESTREAM_QUEUE_TYPE_NAME)
        return {
            "number_of_events": restream_stats["availbleForRestream"],
            "size_used": restream_stats["currentQueueSize"],
            "max_size": restream_stats["maxQueueSize"]
        }

    def get_throughput_by_name(self, name):
        """
        :param name: the name of each node
                ie. Inputs, Code Engine, Restream, Mapper, and Output
        """
        structure = self.get_structure()
        return [x['stats']['throughput'] for x in structure['nodes']
                if x['name'] == name and not x['deleted']]

    def get_incoming_events_count(self, minutes):
        response = self.get_metrics_by_names("INCOMING_EVENTS", minutes)
        return sum(non_empty_datapoint_values(response))

    def get_average_event_size(self, minutes):
        response = self.get_metrics_by_names("EVENT_SIZE_AVG", minutes)
        values = non_empty_datapoint_values(response)
        if not values:
            return 0

        return sum(values)/len(values)

    def get_max_latency(self, minutes):
        try:
            response = self.get_metrics_by_names("LATENCY_MAX", minutes)
            latencies = non_empty_datapoint_values(response)
            if latencies:
                return max(latencies) / 1000
            else:
                return 0
        except Exception as e:
            raise Exception("Failed to get max latency, returning 0. "
                            "Reason: %s", e)

    def create_table(self, table_name, columns):
        """
        :param table_name: self descriptive
        :param columns: self descriptive
        columns example:
        columns = [
        {
            'columnName': 'price', 'distKey': False, 'primaryKey': False,
            'sortKeyIndex': -1,
            'columnType': {'type': 'FLOAT', 'nonNull': False}
        }, {
            'columnName': 'event', 'distKey': True, 'primaryKey': False,
            'sortKeyIndex': 0,
            'columnType': {
                'type': 'VARCHAR', 'length': 256, 'nonNull': False
            }
        }
        ]
        """
        url = self.rest_url + 'tables/' + table_name

        res = self.__send_request(requests.post, url, json=columns)

        return parse_response_to_json(res)

    def alter_table(self, table_name, columns):
        """
        :param table_name: self descriptive
        :param columns: self descriptive
        columns example:
        columns = [
        {
            'columnName': 'price', 'distKey': False, 'primaryKey': False,
            'sortKeyIndex': -1,
            'columnType': {'type': 'FLOAT', 'nonNull': False}
        }, {
            'columnName': 'event', 'distKey': True, 'primaryKey': False,
            'sortKeyIndex': 0,
            'columnType': {
                'type': 'VARCHAR', 'length': 256, 'nonNull': False
            }
        }
        ]
        """
        url = self.rest_url + 'tables/' + table_name

        res = self.__send_request(requests.put, url, json=columns)

        return res

    def get_table_names(self, schema=None):
        """
        :param schema - return tables from a specific schema, else use default
        """
        schema_string = '/%s' % schema if schema is not None else ''
        url = self.rest_url + 'tables%s?shallow=true' % schema_string
        res = self.__send_request(requests.get, url)
        return parse_response_to_json(res)

    # TODO standardize the responses (handling of error code etc)
    def get_tables(self, shallow=False, schema=None):
        """
        :param shallow - only return schema and table names
        :param schema - return tables from a specific schema, else use default
        """
        if shallow:
            return self.get_table_names(schema)

        schema_string = '/%s' % schema if schema is not None else ''
        url = self.rest_url + 'tables%s' % schema_string
        res = self.__send_request(requests.get, url)
        return parse_response_to_json(res)

    def get_notifications(self, epoch_time):
        url = self.rest_url + "notifications?from={epoch_time}". \
            format(epoch_time=epoch_time)
        res = self.__send_request(requests.get, url)
        return parse_response_to_json(res)

    def get_inputs(self, name=None, input_type=None, input_id=None):
        """
        Get a list of all the input nodes in the system
        :param name: Filter by name (accepts Regex)
        :param input_type: Filter by type (e.g. "mysql")
        :param input_id: Filter by node ID
        :return: A list of all the inputs in the system, along
        with metadata and configurations
        """
        url = self.rest_url + 'inputs'
        
        response = self.__send_request(requests.get, url=url)
        nodes = response.json().values()
        
        nodes = [node for node in self.get_plumbing()['nodes']
                 if node['category'] == 'INPUT']
        if input_type:
            nodes = [node for node in nodes if node['type'] == input_type]
        if name:
            regex = re.compile(name)
            nodes = [node for node in nodes if regex.match(node['name'])]
        if input_id:
            nodes = [node for node in nodes if node['id'] == input_id]
        
        return nodes

    def get_output_node(self):
        url = self.rest_url + 'plumbing/outputs'
        res = self.__send_request(requests.get, url)
        return parse_response_to_json(res)[0]

    def set_output(self, output_config, output_name=None):
        """
        Set Output configuration
        :param output_config: :type dict. Output configuration.
            Should contain output-specific configurations
            (see examples below) and the following parameters:
            :param skip_validation: :type bool: True skips output configuration
                                                validation
            :param sink_type: Output type. REDSHIFT, BIGQUERY or SNOWFLAKE
        Redshift example: {"hostname":"redshift-host", "port":5439,
                           "schemaName":"public", "databaseName":"some_db",
                           "username":"user", "password":"password",
                           "skipValidation":false, "sinkType":"REDSHIFT"}
        Snowflake example: {"username":"user", "password":"password",
                            "accountName":"some-account", "warehouseName":"SOME_WH",
                            "databaseName":"SOME_DB", "schemaName":"PUBLIC",
                            "skipValidation":"false", "sinkType":"SNOWFLAKE"}
        BigQuery example: {"projectName":"some-project", "datasetName":"some-dataset",
                           "skipValidation":"false", "sinkType":"BIGQUERY"}
        :param output_name: UI display name
        :return: :type dict. Response's content
        """
        output_node = self.get_output_node()

        current_sink_type = output_node['type'].upper()
        desired_sink_type = output_config['sinkType'].upper()

        if current_sink_type != desired_sink_type:
            raise Exception("Changing output types is not supported. "
                            "Contact support@alooma.io in order "
                            "to change output type "
                            "from {current} to {desired}."
                            .format(current=current_sink_type,
                                    desired=desired_sink_type))
        if 'skipValidation' not in output_config:
            output_config['skipValidation'] = False

        if current_sink_type == OUTPUTS['bigquery']['type']:
            self.__fix_bigquery_config(output_config)

        output_name = output_name if output_name is not None \
            else OUTPUTS[current_sink_type.lower()]['name']

        payload = {
            'configuration': output_config,
            'category': 'OUTPUT',
            'id': output_node['id'],
            'name': output_name,
            'type': current_sink_type,
            'deleted': False
        }
        url = self.rest_url + 'plumbing/nodes/' + output_node['id']
        res = self.__send_request(requests.put, url, json=payload)
        return parse_response_to_json(res)

    def __fix_bigquery_config(self, output_config):
        config_url = self.rest_url + 'zk-configuration/featureUseBigQueryNewConnectConfiguration'
        http_res = self.__send_request(requests.get, config_url)
        json_res = parse_response_to_json(http_res)
        if not json_res['featureUseBigQueryNewLoginConfiguration']:
            output_config['databaseName'] = output_config.pop('projectName')
            output_config['schemaName'] = output_config.pop('datasetName')

    def set_output_config(self, hostname, port, schema_name, database_name,
                          username, password, skip_validation=False,
                          sink_type=None, output_name=None,
                          ssh_server=None, ssh_port=None,
                          ssh_username=None, ssh_password=None):
        """
        DEPRECATED  - use set_output() instead.
        Set Output configuration
        :param hostname: Output hostname
        :param port: Output port
        :param schema_name: Output schema
        :param database_name: Output database name
        :param username: Output username
        :param password: Output password
        :param skip_validation: :type bool: True for skip Output configuration
                                            validation, False for validate
                                            Output configurations
        :param sink_type: Output type. Currently support REDSHIFT, MEMSQL
        :param output_name: Output name that would displayed in the UI
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                             on the SSH server
        :return: :type dict. Response's content
        """
        configuration =  {
            'hostname': hostname,
            'port': port,
            'schemaName': schema_name,
            'databaseName': database_name,
            'username': username,
            'password': password,
            'skipValidation': skip_validation,
            'sinkType': sink_type.upper()
        }
        self._add_ssh_config(configuration, ssh_password, ssh_port,
                             ssh_server, ssh_username)
        return self.set_output(configuration, output_name)

    def get_output_config(self):
        output_node = self.get_output_node()
        if output_node:
            return output_node['configuration']
        return None

    def get_redshift_node(self):
        return self._get_node_by('type',  OUTPUTS['redshift']['type'])

    def set_redshift_config(self, hostname, port, schema_name, database_name,
                            username, password, skip_validation=False,
                            ssh_server=None, ssh_port=None, ssh_username=None,
                            ssh_password=None):
        """
        Set Redshift configuration
        :param hostname: Redshift hostname
        :param port: Redshift port
        :param schema_name: Redshift schema
        :param database_name: Redshift database name
        :param username: Redshift username
        :param password: Redshift password
        :param skip_validation: :type bool: True skips configuration
                                            validation
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                             on the SSH server
        :return: :type dict. Response's content
        """
        configuration = {
            'hostname': hostname,
            'port': port,
            'schemaName': schema_name,
            'databaseName': database_name,
            'username': username,
            'password': password,
            'skipValidation': skip_validation,
            'sinkType': OUTPUTS['redshift']['type']
        }
        self._add_ssh_config(configuration, ssh_password, ssh_port,
                             ssh_server, ssh_username)

        return self.set_output(configuration)

    def _add_ssh_config(self, configuration, ssh_password, ssh_port,
                        ssh_server, ssh_username):
        ssh_config = self.__get_ssh_config(ssh_server=ssh_server,
                                           ssh_port=ssh_port,
                                           ssh_username=ssh_username,
                                           ssh_password=ssh_password)
        if ssh_config:
            configuration['ssh'] = json.dumps(ssh_config) \
                if isinstance(ssh_config, dict) else ssh_config

    def get_redshift_config(self):
        redshift_node = self.get_redshift_node()
        if redshift_node:
            return redshift_node['configuration']
        return None

    def get_snowflake_node(self):
        return self._get_node_by('type',  OUTPUTS['snowflake']['type'])

    def set_snowflake_config(self, account_name, warehouse_name, schema_name,
                             database_name, username, password,
                             skip_validation=False):
        """
        Set Snowflake configuration
        :param account_name: Snowflake account name
        :param warehouse_name: Snowflake warehouse name
        :param schema_name: Snowflake schema
        :param database_name: Snowflake database name
        :param username: Snowflake username
        :param password: Snowflake password
        :param skip_validation: :type bool: True skips configuration
                                            validation
        :return: :type dict. Response's content
        """
        configuration = {
            'warehouseName': warehouse_name,
            'accountName': account_name,
            'schemaName': schema_name,
            'databaseName': database_name,
            'username': username,
            'password': password,
            'skipValidation': skip_validation,
            'sinkType': OUTPUTS['snowflake']['type']
        }
        return self.set_output(configuration)

    def get_snowflake_config(self):
        snowflake_node = self.get_snowflake_node()
        if snowflake_node:
            return snowflake_node['configuration']
        return None

    def get_bigquery_node(self):
        return self._get_node_by('type',  OUTPUTS['bigquery']['type'])

    def set_bigquery_config(self, project_name, dataset_name,
                            skip_validation=False):
        """
        Set BigQuery configuration
        :param schema_name: BigQuery schema
        :param database_name: BigQuery database name
        :param skip_validation: :type bool: True skips configuration
                                            validation
        :return: :type dict. Response's content
        """
        configuration = {
            'projectName': project_name,
            'datasetName': dataset_name,
            'skipValidation': skip_validation,
            'sinkType':  OUTPUTS['bigquery']['type']
        }
        return self.set_output(configuration)

    def get_bigquery_config(self):
        bigquery_node = self.get_bigquery_node()
        if bigquery_node:
            return bigquery_node['configuration']
        return None

    @staticmethod
    def parse_notifications_errors(notifications):
        messages_to_str = "".join(
            [
                notification["typeDescription"] + "\n\t"
                for notification in notifications["messages"]
                if notification["severity"] == "error"
            ]
        )
        return messages_to_str

    def clean_system(self):
        self.set_transform_to_default()
        self.clean_restream_queue()
        self.remove_all_inputs()
        self.delete_all_event_types()
        self.set_settings_email_notifications(
            DEFAULT_SETTINGS_EMAIL_NOTIFICATIONS)
        self.delete_s3_retention()

    def remove_all_inputs(self):
        plumbing = self.get_plumbing()
        for node in plumbing["nodes"]:
            if node["category"] == "INPUT" \
                    and node["type"] not in ["RESTREAM", "AGENT"]:
                self.remove_input(node["id"])

    def delete_all_event_types(self):
        res = self.get_event_types()
        for event_type in res:
            self.delete_event_type(event_type["name"])

    def delete_event_type(self, event_type):
        event_type = urllib.parse.quote(event_type, safe='')
        url = self.rest_url + 'event-types/{event_type}' \
            .format(event_type=event_type)

        self.__send_request(requests.delete, url)

    def get_users(self):
        url = self.rest_url + 'users/'

        res = self.__send_request(requests.get, url)
        return parse_response_to_json(res)

    def get_settings(self):
        url = self.rest_url + 'settings/'

        res = self.__send_request(requests.get, url)
        return parse_response_to_json(res)

    def set_settings_email_notifications(self, email_settings_json):
        url = self.rest_url + "settings/email-notifications"
        self.__send_request(requests.post, url, json=email_settings_json)

    def delete_s3_retention(self):
        url = self.rest_url + "settings/s3-retention"
        self.__send_request(requests.delete, url)

    def clean_restream_queue(self):
        self.purge_restream_queue()

    def purge_restream_queue(self):
        url = self.rest_url + 'plumbing/purge/restream'
        self.__send_request(requests.delete, url)

    def start_restream(self):
        """
        Starts a Restream, streaming data from the Restream Queue
        to the pipeline for processing
        """
        restream_node = self._get_node_by('type', RESTREAM_QUEUE_TYPE_NAME)

        if restream_node:
            restream_id = restream_node["id"]
            url = self.rest_url + "plumbing/nodes/{restream_id}".format(
                restream_id=restream_id)
            restream_click_button_json = {
                "id": restream_id,
                "name": "Restream",
                "type": RESTREAM_QUEUE_TYPE_NAME,
                "configuration": {
                    "streaming": "true"
                },
                "category": "INPUT",
                "deleted": False,
                "state": None
            }
            self.__send_request(requests.put, url,
                                json=restream_click_button_json)
        else:
            raise Exception("Could not find '{restream_type}' type".format(
                restream_type=RESTREAM_QUEUE_TYPE_NAME))

    def get_restream_queue_size(self):
        """
        Returns the number of events currently held in the Restream Queue
        :return: an int representing the number of events in the queue
        """
        restream_node = self._get_node_by("type", RESTREAM_QUEUE_TYPE_NAME)
        return restream_node["stats"]["availbleForRestream"]

    def _get_node_by(self, field, value):
        """
        Get the node by (id, name, type, etc...)
        e.g. _get_node_by("type", "RESTREAM") ->
        :param field: the field to look the node by it
        :param value: tha value of the field
        :return: first node that found, if no node found for this case return
        None
        """
        plumbing = self.get_plumbing()
        for node in plumbing["nodes"]:
            if node[field] == value:
                return node
        return None

    @staticmethod
    def __get_ssh_config(ssh_server, ssh_port,
                         ssh_username, ssh_password=None):
        """
        Get SSH configuration dictionary, for more information:
        https://www.alooma.com/docs/integration/mysql-replication#/#connect-via-ssh
        :param ssh_server: IP or hostname of the destination SSH host
        :param ssh_port: Port of the destination SSH host
        :param ssh_username: Username of the destination SSH host, if not
                         provided we use alooma
        :param ssh_password: Password of the destination SSH host, if not
                             provided we use alooma public key
        :return: :type dict: SSH configuration dictionary
        """
        ssh_config = {}
        if ssh_server and ssh_port and ssh_username:
            ssh_config['server'] = ssh_server
            ssh_config['port'] = ssh_port
            ssh_config['username'] = ssh_username
            if ssh_password:
                ssh_config['password'] = ssh_password

        return ssh_config

    @staticmethod
    def get_public_ssh_key():
        return "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC+t5OKwGcUGYRdDAC8ov" \
               "blV/10AoBfuI/nmkxgRx0J+M3tIdTdxW0Layqb6Xtz8PMsxy1uhM+Rw6cX" \
               "hU/FQWbOr7MB5hJUqXY5OI4NVtI+cc2diCyYUjgCIb7dBSKoyZecJqp3bQ" \
               "uekuZT/OwZ40vLc42g6cUV01b5loV9pU9DvRl6zZXHyrE7fssJ90q2lhvu" \
               "BjltU7g543bUklkYtzwqzYpcynNyrCBSWd85aa/3cVPdiugk7hV4nuUk3m" \
               "VEX/l4GDIsTkLIRzHUHDwt5aWGzhpwdle9D/fxshCbp5nkcg1arSdTveyM" \
               "/PdJJEHh65986tgprbI0Lz+geqYmASgF deploy@alooma.io"

    def get_deployment_info(self):
        """ Return dict with Deployment Info """
        url = self.rest_url + "deployInfo"
        res = self.__send_request(requests.get, url)

        return res.json()

    # SCHEDULED QUERIES #
    def get_scheduled_queries(self):
        """
        Returns all scheduled queries
        :return: a dict representing all scheduled queries
        """
        url = self.rest_url + 'consolidation'
        return requests.get(url, **self.requests_params).json()

    def remove_scheduled_query(self, query_id):
        url = self.rest_url + 'consolidation/' + query_id
        res = self.__send_request(requests.delete, url)
        if not res.ok:
            raise Exception('Failed deleting query id=%s '
                            'status_code=%d response=%s' %
                            (query_id, res.status_code, res.content))

    def get_scheduled_queries_in_error_state(self):
        """
        Returns all scheduled queries that have not successfully ran on
        the last attempt
        :return: a dict representing all failed scheduled queries
        """
        all_queries = self.get_scheduled_queries()
        return {k: all_queries[k] for k in all_queries.keys()
                if all_queries[k]['status'] not in ['active', 'done']}

    def schedule_query(self, event_type, query, frequency=None, run_at=None):
        """ Return Requests Response to Create Query

            :param event_type: Alooma Event Type Related to Query (exiting event
                               type is required, even if just a placeholder)
            :param query: Desired Query to Schedule
            :param frequency: Desired hours between query runs
            :param run_at: Cron like string to run query on

        NOTE: you must select either frequency or run_at
        """
        if frequency is None and run_at is None:
            raise Exception('Must specify either run_at or frequency')

        if event_type is None:
            raise Exception('Event type must be provided')

        scheduled_query_url = self.rest_url + 'custom-consolidation'

        # Prep Data for Consolidation Post
        data = {
            "custom_query": query,
            "event_type": event_type,
            "frequency": frequency,
            "run_at": run_at
        }

        return self.__send_request(requests.post,
                                   scheduled_query_url,
                                   json=data)


class Alooma(Client):
    def __init__(self, hostname, username, password, port=8443,
                 server_prefix=''):
        warnings.warn('%s class is deprecated, passing relevant arguments '
                      'to %s class' % (self.__class__.__name__,
                                       self.__class__.__bases__[0].__name__),
                      DeprecationWarning, stacklevel=2)
        base_url = 'https://%s:%d%s' % (hostname, port, server_prefix)
        super(Alooma, self).__init__(username=username,
                                     password=password,
                                     base_url=base_url)


def response_is_ok(response):
    return 200 <= response.status_code < 300


def parse_response_to_json(response):
    return json.loads(response.content.decode(DEFAULT_ENCODING))


def non_empty_datapoint_values(data):
    """
    From a graphite like response, return the values of the
    non-empty datapoints
    """
    if data:
        return [t[0] for t in data[0]['datapoints'] if t[0]]
    return []


def remove_stats(mapping):
    if 'stats' in mapping:
        del mapping['stats']

    if mapping['fields']:
        for index, field in enumerate(mapping['fields']):
            mapping['fields'][index] = remove_stats(field)
    return mapping
