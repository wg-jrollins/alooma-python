import requests
import json

RESTREAM_QUEUE_TYPE_NAME = "RESTREAM"
REDSHIFT_TYPE = "REDSHIFT"


class _Redshift(object):
    def __init__(self, api):
        self.api = api

    def set_output_config(self, hostname, port, schema_name, database_name,
                          username, password, skip_validation=False,
                          sink_type=None, output_name=None, ssh_server=None,
                          ssh_port=None, ssh_username=None, ssh_password=None):
        """
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
        output_node = self.api.inputs._get_node_by('category', 'OUTPUT')
        name = output_name if output_name is not None else sink_type.title()

        payload = {
            'configuration': {
                'hostname': hostname,
                'port': port,
                'schemaName': schema_name,
                'databaseName': database_name,
                'username': username,
                'password': password,
                'skipValidation': skip_validation,
                'sinkType': sink_type.upper()
            },
            'category': 'OUTPUT',
            'id': output_node['id'],
            'name': name,
            'type': sink_type.upper(),
            'deleted': False
        }
        ssh_config = self.api.inputs.__get_ssh_config(
            ssh_server=ssh_server, ssh_port=ssh_port,
            ssh_username=ssh_username, ssh_password=ssh_password)
        if ssh_config:
            payload['configuration']['ssh'] = json.dumps(ssh_config) \
                if isinstance(ssh_config, dict) else ssh_config

        url = self.api._rest_url + 'plumbing/nodes/' + output_node['id']
        res = self.api._send_request(requests.put, url, json=payload)
        return self.api._parse_response_to_json(res)

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
        :param skip_validation: :type bool: True for skip Redshift configuration
                                            validation, False for validate
                                            Redshift configurations
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
        return self.set_output_config(hostname=hostname, port=port,
                                      schema_name=schema_name,
                                      database_name=database_name,
                                      username=username, password=password,
                                      skip_validation=skip_validation,
                                      sink_type=REDSHIFT_TYPE,
                                      ssh_server=ssh_server,
                                      ssh_port=ssh_port,
                                      ssh_username=ssh_username,
                                      ssh_password=ssh_password)

    def get_redshift_config(self):
        redshift_node = self.api.structure.get_redshift_node()
        if redshift_node:
            return redshift_node['configuration']
        return None

    def get_tables(self):
        """
        Returns a list of the tables in the configured database and schema.
        The list contains the table names and the entire column structure
        including types and constraints
        """
        url = self.api._rest_url + 'tables'
        res = self.api._send_request(requests.get, url)
        return self.api._parse_response_to_json(res)

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
            },
            {
                'columnName': 'event', 'distKey': True, 'primaryKey': False,
                'sortKeyIndex': 0,
                'columnType': {
                    'type': 'VARCHAR', 'length': 256, 'nonNull': False
            }
        ]
        """
        url = self.api._rest_url + 'tables/' + table_name

        res = self.api._send_request(requests.post, url, json=columns)

        return self.api._parse_response_to_json(res)

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
            },
            {
                'columnName': 'event', 'distKey': True, 'primaryKey': False,
                'sortKeyIndex': 0,
                'columnType': {
                    'type': 'VARCHAR', 'length': 256, 'nonNull': False
            }
        ]
        """
        url = self.api._rest_url + 'tables/' + table_name

        res = self.api._send_request(requests.put, url, json=columns)

        return res

SUBMODULE_CLASS = _Redshift
