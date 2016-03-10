import json
import re

import requests
import time
import alooma_exceptions
import alooma


class _Structure(object):
    def __init__(self, api):
        self.__api = api
        self.__send_request = api._Alooma__send_request

    def get_throughput_by_name(self, name):
        structure = self.get_structure()
        return [x['stats']['throughput'] for x in structure['nodes']
                if x['name'] == name and not x['deleted']]

    def get_structure(self):
        url_get = self.__api._rest_url + 'plumbing/?resolution=1min'
        response = self.__send_request(requests.get, url_get)
        return alooma.parse_response_to_json(response)

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
                'loadFiles': load_files,
                'fileFormat': '{"type":"other"}',
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

        url = self.__api._rest_url + 'plumbing/nodes'

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

        raise alooma_exceptions.FailedToCreateInputException(
                'Failed to create {type} input'.format(
                        type=input_post_data["type"]))

    def get_transform_node_id(self):
        transform_node = self._get_node_by('type', 'TRANSFORMER')
        if transform_node:
            return transform_node['id']

        raise Exception('Could not locate transform id for %s' %
                        self.__api.hostname)

    def remove_input(self, input_id):
        url = "{rest_url}plumbing/nodes/remove/{input_id}".format(
                rest_url=self.__api._rest_url, input_id=input_id)
        self.__send_request(requests.post, url)

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

    def get_plumbing(self):
        url = self.__api._rest_url + "plumbing?resolution=30sec"
        res = self.__send_request(requests.get, url)
        return alooma.parse_response_to_json(res)

    def get_inputs(self, name=None, input_type=None, input_id=None):
        """
        Get a list of all the input nodes in the system
        :param name: Filter by name (accepts Regex)
        :param input_type: Filter by type (e.g. "mysql")
        :param input_id: Filter by node ID
        :return: A list of all the inputs in the system, along
        with metadata and configurations
        """
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

    def get_redshift_node(self):
        return self._get_node_by('name', 'Redshift')

    def remove_all_inputs(self):
        plumbing = self.get_plumbing()
        for node in plumbing["nodes"]:
            if node["category"] == "INPUT" \
                    and node["type"] not in ["RESTREAM", "AGENT"]:
                self.remove_input(node["id"])

    def get_input_sleep_time(self, input_id):
        """
        :param input_id:    ID of the input whose sleep time to return
        :return:            sleep time of the input with ID input_id
        """
        url = self.__api._rest_url + 'inputSleepTime/%s' % input_id
        res = requests.get(url, **self.__api.requests_params)
        return float(json.loads(res.content).get('inputSleepTime'))

    def set_input_sleep_time(self, input_id, sleep_time):
        """
        :param input_id:    ID of the input whose sleep time to change
        :param sleep_time:  new sleep time to set for input with ID input_id
        :return:            result of the REST request
        """
        url = self.__api._rest_url + 'inputSleepTime/%s' % input_id
        res = requests.put(url, str(sleep_time), **self.__api.requests_params)
        return res
