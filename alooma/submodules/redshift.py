import requests


class _Redshift(object):
    def __init__(self, api):
        self.__api = api

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
        url = self.__api._rest_url + 'plumbing/nodes/'+redshift_node['id']

        res = self.__api._send_request(requests.put, url, json=payload)
        return self.__api._parse_response_to_json(res)

    def get_redshift_config(self):
        redshift_node = self.__api.structure.get_redshift_node()
        if redshift_node:
            return redshift_node['configuration']
        return None

    def get_tables(self):
        """
        Returns a list of the tables in the configured database and schema.
        The list contains the table names and the entire column structure
        including types and constraints
        """
        url = self.__api._rest_url + 'tables'
        res = self.__api._send_request(requests.get, url)
        return self.__api._parse_response_to_json(res)

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
        url = self.__api._rest_url + 'tables/' + table_name

        res = self.__api._send_request(requests.post, url, json=columns)

        return self.__api._parse_response_to_json(res)

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
        url = self.__api._rest_url + 'tables/' + table_name

        res = self.__api._send_request(requests.put, url, json=columns)

        return res

SUBMODULE_CLASS = _Redshift
