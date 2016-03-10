import requests

import alooma


class _Redshift(object):
    def __init__(self, api):
        self.__api = api
        self.__send_request = api._Alooma__send_request

    def set_redshift_config(self, hostname, port, schema_name, database_name,
                            username, password, skip_validation=False):
        redshift_node = self.__api.structure.get_redshift_node()
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
        url = self.__api.rest_url + 'plumbing/nodes/'+redshift_node['id']

        res = self.__api.__send_request(requests.put, url, json=payload)
        return alooma.parse_response_to_json(res)

    def get_redshift_config(self):
        redshift_node = self.__api.structure.get_redshift_node()
        if redshift_node:
            return redshift_node['configuration']
        return None

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
        url = self.__api.rest_url + 'tables/' + table_name

        res = self.__send_request(requests.post, url, json=columns)

        return alooma.parse_response_to_json(res)

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
        url = self.__api.rest_url + 'tables/' + table_name

        res = self.__api.__send_request(requests.put, url, json=columns)

        return res
