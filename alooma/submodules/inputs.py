import json
import re
import time

import requests

import alooma_exceptions

POST_DATA_CONFIGURATION = 'configuration'
POST_DATA_NAME = 'name'
POST_DATA_TYPE = 'type'


class _Structure(object):
    def __init__(self, api):
        self.__api = api

    def get_throughput_by_name(self, name):
        structure = self.get_structure()
        return [x['stats']['throughput'] for x in structure['nodes']
                if x['name'] == name and not x['deleted']]

    def create_mongodb_input(self, name, hostname, database):
        configuration = {"hostname": hostname, "database": database}
        return self.create_input(configuration=configuration,
                                 input_type="MONGODB",
                                 input_name=name)

    def create_mixpanel_input(self, name, mixpanel_api_key, mixpanel_api_secret,
                              from_date):
        configuration = {
            "mixpanelApiKey": mixpanel_api_key,
            "mixpanelApiSecret": mixpanel_api_secret,
            "fromDate": from_date
        }

        return self.create_input(configuration=configuration, input_name=name,
                                 input_type="MIXPANEL")

    def create_s3_input(self, name, key, secret, bucket, prefix='',
                        load_files='all', file_format="json", delimiter=",",
                        quote_char="", escape_char=""):
        formats = ["json", "delimited", "other"]
        if file_format not in formats:
            raise ValueError("File format cannot be '{file_format}', it has to "
                             "be one of those: {formats}"
                             .format(file_format=file_format,
                                     formats=", ".join(formats)))

        file_format_config = {"type": file_format}
        if file_format == "delimited":
            for key, value in {"delimiter": delimiter,
                               "quoteChar": quote_char,
                               "escapeChar": escape_char}.items():
                if value:
                    file_format_config[key] = value

        configuration = {
            "awsAccessKeyId": key,
            "awsBucketName": bucket,
            "awsSecretAccessKey": secret,
            "filePrefix": prefix,
            "loadFiles": load_files,
            "fileFormat": json.dumps(file_format_config)
        }

        return self.create_input(configuration=configuration,
                                 input_name=name,
                                 input_type="S3")

    def create_azure_input(self, name, account_name, account_key,
                           container_name, file_prefix, load_files):
        configuration = {
            "azureAccountName": account_name,
            "azureAccountKey": account_key,
            "azureContainerName": container_name,
            "azureFilePrefix": file_prefix,
            "loadFiles": load_files
        }
        self.create_input(configuration=configuration, input_name=name,
                          input_type="AZURE_STORAGE")

    def create_localytics_input(self, name, aws_access_key_id,
                                aws_secret_access_key, company_name, app_id):
        configuration = {
            "awsAccessKeyId": aws_access_key_id,
            "awsSecretAccessKey": aws_secret_access_key,
            "companyName": company_name,
            "appId": app_id
        }
        self.create_input(configuration=configuration, input_name=name,
                          input_type="LOCALYTICS")

    def create_input(self, configuration, input_name, input_type):
        if input_type not in self.get_input_types():
            raise alooma_exceptions.FailedToCreateInputException(
                "Input type must be one of '%s', you tried to add '%s'" % (
                    ", ".join(self.get_input_types()), input_type))
        post_data = {
            POST_DATA_NAME: input_name,
            POST_DATA_TYPE: input_type,
            POST_DATA_CONFIGURATION: configuration
        }
        structure = self.get_structure()
        previous_nodes = [x for x in structure['nodes']
                          if x[POST_DATA_NAME] == post_data[POST_DATA_NAME]]

        url = self.__api._rest_url + 'plumbing/nodes'

        self.__api._send_request(requests.post, url, json=post_data)

        new_id = None
        retries_left = 10
        while retries_left > 0:
            retries_left -= 1
            structure = self.get_structure()
            input_type_nodes = [
                x for x in structure['nodes']
                if x[POST_DATA_NAME] == post_data[POST_DATA_NAME]]
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
                'Failed to create {type} input'
                .format(type=post_data[POST_DATA_TYPE]))

    def get_transform_node_id(self):
        transform_node = self._get_node_by('type', 'TRANSFORMER')
        if transform_node:
            return transform_node['id']

        raise Exception('Could not locate transform id for %s' %
                        self.__api.hostname)

    def remove_input(self, input_id):
        url = "{rest_url}plumbing/nodes/remove/{input_id}".format(
                rest_url=self.__api._rest_url, input_id=input_id)
        self.__api._send_request(requests.post, url)

    def _get_node_by(self, field, value):
        """
        Get the node by (id, name, type, etc...)
        e.g. _get_node_by("type", "RESTREAM") ->
        :param field: the field to look the node by it
        :param value: tha value of the field
        :return: first node that found, if no node found for this case return
        None
        """
        plumbing = self.get_structure()
        for node in plumbing["nodes"]:
            if node[field] == value:
                return node
        return None

    def get_structure(self):
        """
        Returns a representation of all the inputs, outputs,
        and on-stream processors currently configured in the system
        :return: A dict representing the structure of the system
        """
        url_get = self.__api._rest_url + 'plumbing/?resolution=1min'
        response = self.__api._send_request(requests.get, url_get)
        return self.__api._parse_response_to_json(response)

    def get_plumbing(self):
        """
        DEPRECATED - use get_structure() instead.
        Returns a representation of all the inputs, outputs,
        and on-stream processors currently configured in the system
        :return: A dict representing the structure of the system
        """
        return self.get_structure()

    def get_inputs(self, name=None, input_type=None, input_id=None):
        """
        Get a list of all the input nodes in the system
        :param name: Filter by name (accepts Regex)
        :param input_type: Filter by type (e.g. "mysql")
        :param input_id: Filter by node ID
        :return: A list of all the inputs in the system, along
        with metadata and configurations
        """
        nodes = [node for node in self.get_structure()['nodes']
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
        plumbing = self.get_structure()
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
        res = self.__api._send_request(requests.get, url)
        return float(json.loads(res.content).get('inputSleepTime'))

    def set_input_sleep_time(self, input_id, sleep_time):
        """
        :param input_id:    ID of the input whose sleep time to change
        :param sleep_time:  new sleep time to set for input with ID input_id
        :return:            result of the REST request
        """
        url = self.__api._rest_url + 'inputSleepTime/%s' % input_id
        res = self.__api._send_request(requests.put, url, data=str(sleep_time))
        return res

    def get_input_structure(self, name):
        url = "%splumbing/nodes/%s" % (
            self.__api._rest_url, self._get_input_node_ids(name))
        res = self.__api._send_request(requests.get, url)
        return json.loads(res.content)

    def get_input_types(self):
        url = self.__api._rest_url + "plumbing/types"
        res = self.__api._send_request(requests.get, url).json()
        return res["INPUT"]

    def _get_input_node_ids(self, name):
        return self._get_node_by("name", name)["id"]

SUBMODULE_CLASS = _Structure
