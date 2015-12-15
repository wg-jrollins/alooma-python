import json
import time
import requests
import urllib

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

RESTREAM_QUEUE_TYPE_NAME = "RESTREAM"


class FailedToCreateInputException(Exception):
    pass


class Alooma(object):
    def __init__(self, hostname, username, password, port=8443,
                 server_prefix=''):

        self.hostname = hostname
        self.rest_url = 'https://%s:%d%s/rest/' % (hostname,
                                                   port,
                                                   server_prefix)
        self.segment_url = 'http://%s%s/segment' % (hostname,
                                                    server_prefix)
        self.cookie = self.__login(username, password)
        if not self.cookie:
            raise Exception('Failed to obtain cookie')

        self.requests_params = {'timeout': 20,
                                'cookies': self.cookie,
                                'verify': False}

    def __login(self, username, password):
        print("Attempting to login and obtain a session cookie from %s..." %
              format(self.hostname))
        url = self.rest_url + 'login'
        login_data = {"email": username, "password": password}
        response = requests.post(url, json=login_data)
        if response.status_code == 200:
            return response.cookies
        else:
            raise Exception('Failed to login to {} with username: '
                            '{}'.format(self.hostname, username))

    def get_config(self):
        url_get = self.rest_url + 'config/export'
        response = requests.get(url=url_get, **self.requests_params)
        config_export = json.loads(response.content.decode())
        return config_export

    def get_structure(self):
        url_get = self.rest_url + 'plumbing/?resolution=1min'
        response = requests.get(url=url_get, **self.requests_params)
        structure = json.loads(response.content.decode())
        return structure

    def get_mapping_mode(self):
        url = self.rest_url + 'mapping-mode'
        res = requests.get(url, **self.requests_params)
        return res.content

    def get_mapping(self, event_type):
        event_type = self.get_event_type(event_type)
        mapping = remove_stats(event_type)
        return mapping

    def create_s3_input(self, name, key, secret, bucket, prefix,
                        load_files, transform_id):
        post_data = {
            'source': None,
            'target': str(transform_id),
            'name': name,
            'type': 'S3',
            'configuration': {
                'awsAccessKeyId': key,
                'awsBucketName': bucket,
                'awsSecretAccessKey': secret,
                'filePrefix': prefix,
                'loadFiles': load_files
            }
        }
        return self.create_input(input_post_data=post_data)

    def create_mixpanel_input(self, mixpanel_api_key, mixpanel_api_secret,
                              from_date, name, transform_id):
        post_data = {
            "source": None,
            "target": str(transform_id),
            "name": name,
            "type": "MIXPANEL",
            "configuration": {
                "mixpanelApiKey": mixpanel_api_key,
                "mixpanelApiSecret": mixpanel_api_secret,
                "fromDate": from_date
            }
        }
        return self.create_input(input_post_data=post_data)

    def create_input(self, input_post_data):
        structure = self.get_structure()
        previous_nodes = [x for x in structure['nodes']
                          if x['name'] == input_post_data['name']]

        url = self.rest_url + 'plumbing/nodes'
        res = requests.post(url=url, json=input_post_data,
                            **self.requests_params)
        if not response_is_ok(res):
            print("Failed to create input due reason {reason}"
                  "".format(reason=res.reason))
            return
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

    def get_transform_node_id(self):
        plumbing = self.get_plumbing()
        nodes = plumbing['nodes']
        for node in nodes:
            if node['type'] == 'TRANSFORMER':
                return node['id']

        raise Exception('Could not locate transform id for %s' %
                        self.hostname)

    def remove_input(self, input_id):
        url = "{rest_url}plumbing/nodes/remove/{input_id}".format(
            rest_url=self.rest_url, input_id=input_id)
        res = requests.post(url=url, **self.requests_params)
        if not response_is_ok(res):
            print("Could not remove input due to reason: \n"
                  "{reason}".format(reason=res.reason))

    def set_transform_to_default(self):
        transform = DEFAULT_TRANSFORM_CODE
        self.set_transform(transform=transform)

    def set_mapping(self, mapping, event_type):
        url = self.rest_url + 'event-types/{event_type}/mapping'.format(
            event_type=event_type)
        res = requests.post(url, json=mapping, **self.requests_params)
        return res

    def set_mapping_mode(self, flexible):
        url = self.rest_url + 'mapping-mode'
        res = requests.post(url, json='FLEXIBLE' if flexible else 'STRICT',
                            **self.requests_params)
        return res

    def set_classifier_by_field(self, field_name):
        url = self.rest_url + "type-classifier"
        res = requests.put(url, json={
            "BY_FIELD": {
                "fieldName": field_name
            }
        }, cookies=self.cookie)
        if not response_is_ok(res):
            print(res.reason)

    def discard_event_type(self, event_type):
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

    def map_field(self, schema, field_path, column_name, field_type, non_null,
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

        field = self.find_field_name(schema, field_path, True)
        self.set_mapping_for_field(field, column_name, field_type,
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

    def find_field_name(self, schema, field_path, add_if_missing=False):
        """
        :param schema:  this is the dict that this method run over ot
                        recursively
        :param field_path: this would use us to find the keys
        :param add_if_missing: add the field if missing
        :return:    the field that we wanna find and to do on it some changes.
                    if the field is not found then return None and print it
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
            return self.find_field_name(field, remaining_path[0])
        elif add_if_missing:
            parent_field = schema
            for field in fields_list:
                parent_field = self.add_field(parent_field, field)
            return parent_field
        else:
            # print this if the field is not found,
            # not standing with the case ->
            # field["fieldName"] == field_to_find
            print("Could not find field path")

    def set_input_sleep_time(self, input_id, sleep_time):
        url = self.rest_url + 'inputSleepTime/%s' % input_id
        res = requests.put(url, str(sleep_time), **self.requests_params)
        return res

    def get_transform(self):
        url = self.rest_url + 'transform/functions/main'
        res = requests.get(url, **self.requests_params)
        return json.loads(res.content.decode())["code"]

    def set_transform(self, transform):
        data = {'language': 'PYTHON', 'code': transform,
                'functionName': 'main'}
        url = self.rest_url + 'transform/functions/main'
        res = requests.post(url, json=data, **self.requests_params)
        return res

    def get_incoming_queue_metric(self, minutes):
        url = self.rest_url + 'metrics?metrics=EVENTS_IN_PIPELINE' \
                              '&from=-%dmin&resolution=%dmin' % (
                                  minutes, minutes)
        response = json.loads(
            requests.get(url, **self.requests_params).content.decode())
        incoming = non_empty_datapoint_values(response)
        if incoming:
            return max(incoming)
        else:
            return 0

    def get_outputs_metrics(self, minutes):
        """
        :param minutes: amount of minutes
        Returns a 4-tuple containing the number of events unmapped, discarded,
        errored and loaded
        """
        url = self.rest_url + 'metrics?metrics=UNMAPPED_EVENTS,IGNORED_EVENTS,'\
                              'ERROR_EVENTS,LOADED_EVENTS_RATE' \
                              '&from=-%dmin&resolution=%dmin' % (
                                  minutes, minutes)
        response = json.loads(
            requests.get(url, **self.requests_params).content)
        return tuple([sum(non_empty_datapoint_values([r])) for r in response])

    def get_throughput_by_name(self, name):
        structure = self.get_structure()
        return [x['stats']['throughput'] for x in structure['nodes']
                if x['name'] == name and not x['deleted']]

    def get_incoming_events_count(self, minutes):
        url = self.rest_url + 'metrics?metrics=INCOMING_EVENTS&from=-' \
                              '%dmin&resolution=%dmin' % (minutes, minutes)
        response = json.loads(
            requests.get(url, **self.requests_params).content.decode())
        return sum(non_empty_datapoint_values(response))

    def get_average_event_size(self, minutes):
        url = self.rest_url + 'metrics?metrics=EVENT_SIZE_AVG&from=-' \
                              '%dmin&resolution=%dmin' % (minutes, minutes)
        response = json.loads(
            requests.get(url, **self.requests_params).content.decode())

        values = non_empty_datapoint_values(response)
        if not values:
            return 0

        return sum(values)/len(values)

    def get_max_latency(self, minutes):
        url = self.rest_url + 'metrics?metrics=LATENCY_MAX&from=' \
                              '-%dmin&resolution=%dmin' % (minutes, minutes)
        try:
            response = json.loads(
                requests.get(url, **self.requests_params).content.decode())
            latencies = non_empty_datapoint_values(response)
            if latencies:
                return max(latencies) / 1000
            else:
                return 0
        except Exception as e:
            print ("Failed to get max latency, returning 0. Reason: %s", e)
            return 0

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

        res = requests.post(url, json=columns, **self.requests_params)

        if res.status_code not in [204, 200]:
            print("Could not create table due to - {exception}".format(
                    exception=res.reason))

        return json.loads(res.content.decode())

    # TODO standardize the responses (handling of error code etc)
    def get_tables(self):
        url = self.rest_url + 'tables'
        res = requests.get(url, cookies=self.cookie)
        return res

    def create_table_one_column(self, table_name, column_name, column_type,
                                non_null, primary_key, dist_key, sort_key,
                                **type_attrs):
        table_json = [{
            'columnName': column_name,
            'columnType': {
                'type': column_type,
                'nonNull': str(non_null).lower(),
            },
            'primaryKey': str(primary_key).lower(),
            'distKey': str(dist_key).lower(),
            'sortKeyIndex': 0 if sort_key else -1,
        }]
        for k, v in type_attrs.items():
            table_json[0]['columnType'][k] = v
        url = self.rest_url + 'tables/' + table_name
        res = requests.post(url, json=table_json, cookies=self.cookie)
        return res

    def get_notifications(self, epoch_time):
        url = self.rest_url + "notifications?from={epoch_time}". \
            format(epoch_time=epoch_time)
        res = requests.get(url, cookies=self.cookie)
        if res.status_code not in [200, 204]:
            print("Failed to get notifications")
        else:
            res = json.loads(res.content.decode())
            return res

    def stream_segment(self, json_content):
        res = requests.post(self.segment_url,
                            json=json_content,
                            headers={"Content-Type": "application/json"})
        if not response_is_ok(res):
            print(res.reason)
        return res

    def get_plumbing(self):
        url = self.rest_url + "/plumbing?resolution=30sec"
        res = requests.get(url, cookies=self.cookie)
        return json.loads(res.content.decode())

    def get_redshift_node(self):
        plumbing = self.get_plumbing()
        for node in plumbing['nodes']:
            if node['name'] == 'Redshift':
                return node
        return None

    def set_redshift_config(self, hostname, port, schema_name, database_name,
                            username, password, skip_validation=False):
        redshift_node = self.get_redshift_node()
        print(redshift_node)
        payload = {
            'configuration': {
                'hostname': hostname,
                'port': port,
                'schemaName': schema_name,
                'databaseName': database_name,
                'username': username,
                'password': password,
                'skipValidation': skip_validation
                },
            'category': 'OUTPUT',
            'id': redshift_node['id'],
            'name': 'Redshift',
            'type': 'REDSHIFT',
            'deleted': False
        }
        url = self.rest_url + '/plumbing/nodes/'+redshift_node['id']
        res = requests.put(url=url, json=payload, **self.requests_params)
        if res.status_code not in [204, 200]:
            print("Could not configure Redshift due to - {exception}".format(
                exception=res.reason))
        return json.loads(res.content.decode())

    def get_redshift_config(self):
        redshift_node = self.get_redshift_node()
        if redshift_node:
            return redshift_node['configuration']
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
        self.set_mapping_mode(flexible=False)
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
        if hasattr(urllib, "parse"):
            event_type = urllib.parse.quote_plus(event_type)
        else:
            event_type = urllib.quote_plus(event_type)

        url = self.rest_url + '/event-types/{event_type}'\
            .format(event_type=event_type)
        res = requests.delete(url,  **self.requests_params)
        if res.status_code not in [204, 200]:
            raise "Could not delete event type -{event_type} due to - " \
                  "{exception}".format(
                      exception=res.reason, event_type=event_type)

    def get_event_types(self):
        url = self.rest_url + '/event-types'
        res = requests.get(url=url, **self.requests_params)
        if res.status_code not in [204, 200]:
            print("Could not get event types due to - {exception}".format(
                exception=res.reason))
        return json.loads(res.content.decode())

    def get_event_type(self, event_type):
        if hasattr(urllib, "parse"):
            event_type = urllib.parse.quote_plus(event_type)
        else:
            event_type = urllib.quote_plus(event_type)

        url = self.rest_url + '/event-types/' + urllib.quote_plus(
            event_type)
        res = requests.get(url=url, **self.requests_params)
        if res.status_code not in [204, 200]:
            print("Could not get event type due to - {exception}".format(
                exception=res.reason))
        return json.loads(res.content.decode())

    def set_classifier_by_input(self):
        classifier_json = {"BY_SOURCE": {}}
        url = self.rest_url + "/type-classifier"
        res = requests.put(url, json=classifier_json,
                           **self.requests_params)
        if res.status_code not in [204, 200]:
            print("Could not set classify by input due to - {exception}".format(
                exception=res.reason))

    def set_settings_email_notifications(self, email_settings_json):
        url = self.rest_url + "/settings/email-notifications"
        res = requests.post(url, json=email_settings_json,
                            **self.requests_params)
        if res.status_code not in [204, 200]:
            print("Could not set email notifications settings due to - "
                  "{exception}".format(exception=res.reason))

    def delete_s3_retention(self):
        url = self.rest_url + "/settings/s3-retention"
        res = requests.delete(url, **self.requests_params)
        if res.status_code not in [204, 200]:
            print("Could not set s3 retention settings due to - "
                  "{exception}".format(exception=res.reason))

    def clean_restream_queue(self):
        event_types = self.get_event_types()
        for event_type in event_types:
            self.discard_event_type(event_type["name"])

        self.start_restream()
        queue_depth = self.get_restream_queue_size()
        while queue_depth != 0:
            queue_depth = self.get_restream_queue_size()
            time.sleep(1)

    def start_restream(self):
        plumbing = self.get_plumbing()
        restream_id = None
        for node in plumbing["nodes"]:
            if node["type"] == RESTREAM_QUEUE_TYPE_NAME:
                restream_id = node["id"]

        if restream_id:
            url = self.rest_url + "/plumbing/nodes/{restream_id}".format(
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
            res = requests.put(url, json=restream_click_button_json,
                               **self.requests_params)
            if res.status_code not in [204, 200]:
                print("Could not click restream button due to - {exception}"
                      .format(exception=res.reason))
        else:
            print("Could not find '{restream_type}' type".format(
                restream_type=RESTREAM_QUEUE_TYPE_NAME))

    def get_restream_queue_size(self):
        plumbing = self.get_plumbing()
        for node in plumbing["nodes"]:
            if node["type"] == RESTREAM_QUEUE_TYPE_NAME:
                return node["stats"]["availbleForRestream"]

    def get_restream_input(self):
        plumbing = self.get_plumbing()
        for node in plumbing["nodes"]:
            if node["type"] == RESTREAM_QUEUE_TYPE_NAME:
                return node


def response_is_ok(response):
    return 200 <= response.status_code < 300


def non_empty_datapoint_values(data):
    """
    :param data: self descriptive
    From a graphite like response, return the values of the non-empty datapoints
    """
    if data:
        return [t[0] for t in data[0]['datapoints'] if t[0]]
    return []


def remove_stats(mapping):
    if mapping['stats']:
        del mapping['stats']

    if mapping['fields']:
        for index, field in enumerate(mapping['fields']):
            mapping['fields'][index] = remove_stats(field)
    return mapping
