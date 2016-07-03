import datetime
import json
import re
import time
import urllib

import requests

from alooma import alooma_exceptions
from alooma.consts import POST_DATA_CONFIGURATION, POST_DATA_NAME, \
    POST_DATA_TYPE, DATE_FORMAT, DATETIME_FORMAT


class Structure(object):
    def __init__(self, api):
        self.api = api

    def get_throughput_by_name(self, name):
        structure = self.get_structure()
        return [x['stats']['throughput'] for x in structure['nodes']
                if x['name'] == name and not x['deleted']]

    def create_azure_input(self, input_name, account_name, account_key,
                           container_name, file_prefix, load_files="new",
                           one_click=False):
        """
        Create a new azure input
        :param input_name: Name your new input
        :param account_name: Azure Account Name
        :param account_key: Azure Account Key
        :param container_name: Container Name
        :param file_prefix: File prefix (optional)
        :param load_files: Import files (optional - must be "new", "all")
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping. 
        """
        load_files_options = ["new", "all"]
        if load_files not in load_files_options:
            raise ValueError("load_files value most be one of the above %s."
                             % ", ".join(load_files_options))

        configuration = {
            "azureAccountName": account_name,
            "azureAccountKey": account_key,
            "azureContainerName": container_name,
            "azureFilePrefix": file_prefix,
            "loadFiles": load_files
        }
        self.create_input(configuration=configuration, input_name=input_name,
                          input_type="AZURE_STORAGE", one_click=one_click)

    def create_rest_input(self, input_name, one_click=False):
        """
        Create a new REST input
        :param input_name: Name your new input
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping. 
        """
        self.create_input(configuration={}, input_name=input_name,
                          input_type="RESTAPI", one_click=one_click)

    def create_elasticserarch_input(self, input_name, hostname, index, query="",
                                    port=9200, replication_frequency=8,
                                    ssh_server=None, ssh_port=None,
                                    ssh_username=None, ssh_password=None,
                                    one_click=False):
        """
        Create a new Elasticsearch input
        :param input_name: Name your new input
        :param hostname: Hostname or IP address of your Elasticsearch server
        :param index: The name of the Elasticsearch index
        :param query: An optional JSON query to be run on the index
        :param port: Elasticsearch server's port
        :param replication_frequency: Replication Frequency, every
                                      replication_frequency hours
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                            from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                          the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                              SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        """
        configuration = {
            "port": port,
            "pull_interval": replication_frequency,
            "hostname": hostname,
            "index": index,
            "query": query
        }
        self.create_input(input_name=input_name, configuration=configuration,
                          ssh_password=ssh_password, ssh_server=ssh_server,
                          ssh_port=ssh_port, ssh_username=ssh_username,
                          one_click=one_click, input_type='ELASTICSEARCH_INPUT')

    def create_mobile_sdk_input(self, input_name, one_click=False):
        """
        Create a new Mobile SDK input
        :param input_name: Name your new input
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping. 
        """
        self.create_input(configuration={}, input_name=input_name,
                          one_click=one_click, input_type="MOBILE_SDK")

    def create_java_sdk_input(self, input_name, one_click=False):
        """
        Create a new JAVA SDK input
        :param input_name: Name your new input
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping. 
        """
        self.create_input(configuration={}, input_name=input_name,
                          one_click=one_click, input_type="JAVA_SDK")

    def create_javascript_sdk_input(self, input_name, one_click=False):
        """
        Create a new JavaScript SDK input
        :param input_name: Name your new input
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping. 
        """
        self.create_input(configuration={}, input_name=input_name,
                          one_click=one_click, input_type="JSSDK")

    def create_localytics_input(self, input_name, aws_access_key_id,
                                aws_secret_access_key, bucket_name,
                                company_name, app_id, starting_from=None,
                                end_date=None, one_click=False):
        """
        Create a new Localytics input
        :param input_name: Name your new input
        :param aws_access_key_id: AWS Access Key ID
        :param aws_secret_access_key: AWS Secret Access Key
        :param bucket_name: Bucket Name
        :param company_name: Company Name
        :param app_id: App ID
        :param starting_from: If you also want to import historical events then
                              pass datetime.date (utc time) object (Optional)
        :param end_date: If you want to import events until and including then
                         pass datetime.date (utc time) object (Optional)
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping. 
        """
        configuration = {
            'bucketName': bucket_name,
            'awsAccessKeyId': aws_access_key_id,
            "awsSecretAccessKey": aws_secret_access_key,
            "companyName": company_name,
            "appId": app_id
        }
        if end_date:
            configuration["fromDate"] = end_date.strftime(DATE_FORMAT)
        if starting_from:
            configuration["toDate"] = starting_from.strftime(DATE_FORMAT)

        self.create_input(configuration=configuration, input_name=input_name,
                          input_type="LOCALYTICS", one_click=one_click)

    def create_mssql_incremental_dump_load_input(self, input_name, server, port,
                                                 user, password, database,
                                                 schema, tables,
                                                 ssh_server=None,
                                                 ssh_port=None,
                                                 ssh_username=None,
                                                 ssh_password=None,
                                                 batch_size=10000,
                                                 one_click=False):
        """
        Create a new Microsoft SQL Server incremental dump\load input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your Microsoft SQL server

        :param port: Microsoft SQL server's port
        :param user: User name to use when connecting to your Microsoft SQL
                     server
        :param password: Password to use when connecting to your Microsoft SQL
                         server
        :param database: Database name to replicate
        :param schema: Schema name
        :param tables: Tables to replicate. :type dict which it's keys are the
                       tables names and the values are the update indicator
                       columns
         :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                            from the public internet
         :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                          the public internet (default port is 22)
         :param ssh_username: (Optional) The user name on the SSH server for the
                              SSH connection (the standard is alooma)
         :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param batch_size: :type int
                           Bigger batch size means faster backfill, but may
                           increase the load on your Microsoft SQL server.
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        self.create_incremental_dump_load_input(
            db_type="mssql", input_name=input_name, server=server, port=port,
            user=user, password=password, database=database, schema=schema,
            tables=tables, ssh_server=ssh_server, ssh_port=ssh_port,
            ssh_username=ssh_username, ssh_password=ssh_password,
            batch_size=batch_size, one_click=one_click)

    def create_mssql_full_dump_load_replication_input(
            self, input_name, server, port, user, password, database, schema,
            tables, replication_frequency=5, start_time=datetime.time(5),
            ssh_server=None,  ssh_port=None, ssh_username=None,
            ssh_password=None, one_click=False):
        """
        Create a new Microsoft SQL Server full dump\load input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your Microsoft SQL server

        :param port: Microsoft SQL server's port
        :param user: User name to use when connecting to your Microsoft SQL
                     server
        :param password: Password to use when connecting to your Microsoft SQL
                         server
        :param database: Database name to replicate
        :param schema: Schema name
        :param tables: List of Tables to Replicate :type list or str space
                                                         separated list of table
                                                         names
        :param replication_frequency: Every how much time to replicate tables
        :param start_time: When to start running replication :type datetime
                           object that contains time attributes
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        self.create_full_dump_load_replication_input(
            'mssql', input_name=input_name, port=port, tables=tables,
            replication_frequency=replication_frequency, start_time=start_time,
            server=server, user=user, password=password, database=database,
            schema=schema, ssh_server=ssh_server, ssh_port=ssh_port,
            ssh_username=ssh_username, ssh_password=ssh_password,
            one_click=one_click)

    def create_mixpanel_input(self, input_name, mixpanel_api_key,
                              mixpanel_api_secret,
                              from_date=None, to_date=None,
                              one_click=False):
        """
        Create a new Mixpanel input
        :param input_name: Name your new input
        :param mixpanel_api_key: API key (32 characters)
        :param mixpanel_api_secret: API secret (32 characters)
        :param from_date: If you want to import events until and including then
                          pass datetime.date (utc time) object (Optional)
        :param to_date: If you also want to events until specific date then
                        pass datetime.date (utc time) object (Optional)
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """

        configuration = {
            "mixpanelApiKey": mixpanel_api_key.lower(),
            "mixpanelApiSecret": mixpanel_api_secret.lower()
        }

        if from_date:
            configuration["fromDate"] = from_date.strftime(DATE_FORMAT)

        if to_date:
            configuration["toDate"] = to_date.strftime(DATE_FORMAT)

        self.create_input(configuration=configuration, input_name=input_name,
                          input_type="MIXPANEL", one_click=one_click)

    def create_mongodb_input(self, input_name, hostname, port, database,
                             username=None, password=None,
                             additional_connection_string=None,
                             collections_to_replicate=None,
                             start_time=None, ssh_server=None,
                             ssh_port=None, ssh_username=None,
                             ssh_password=None, one_click=False):
        """
        Create a new MongoDB input
        :param input_name: Name your new input
        :param hostname: Hostname or IP address of your MongoDB server
        :param port: MongoDB server's port 
        :param database: Database name to replicate
        :param username: Username used to connect to your MongoDB server
                         (optional)
        :param password: Password used to connect to your MongoDB server
                         (optional)
        :param additional_connection_string: This input box allows you to add
                                             any valid MongoDB connection string
                                             parameter to the input (optional)
        :param collections_to_replicate: A space-separated list of collections
                                         to monitor in the database. Leave blank
                                         for all collections
        :param start_time: If you also want to import historical events then
                           pass datetime.date (utc time) object (Optional)
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        configuration = {
            'port': port,
            'hostname': hostname,
            'database': database,
            'advanced_parameters': additional_connection_string,
            'collections': collections_to_replicate
        }
        if username:
            configuration['username'] = username
        if password:
            configuration['password'] = password
        if additional_connection_string:
            configuration['advanced_parameters'] = additional_connection_string
        if collections_to_replicate:
            configuration['collections'] = collections_to_replicate
        if start_time:
            configuration['start_time'] = start_time.strftime(DATETIME_FORMAT)

        self.create_input(configuration=configuration, input_type='MONGODB',
                          input_name=input_name, one_click=one_click,
                          ssh_server=ssh_server, ssh_port=ssh_port,
                          ssh_username=ssh_username, ssh_password=ssh_password)

    def create_mysql_incremental_dump_load_input(
            self, input_name, server, port, user, password, tables,
            database=None, schema=None, ssh_server=None, ssh_port=None,
            ssh_username=None, ssh_password=None, batch_size=10000,
            one_click=False):
        """
        Create a new MySQL incremental dump\load input
        :param input_name: :type str. Name your new input
        :param server: Hostname or IP address of your Microsoft SQL server

        :param port: MySQL server's port
        :param user: User name to use when connecting to your
                     MySQL server
        :param password: Password to use when connecting to your
                         MySQL server
        :param database: Database name to replicate
        :param schema: Schema name
        :param tables: Tables to replicate. :type dict which it's keys are the
                       tables names and the values are the update indicator
                       columns
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param batch_size: :type int
                           Bigger batch size means faster backfill, but may
                           increase the load on your MySQL.
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        if database is None and schema is None:
            raise TypeError("Both database and schema are None. You have to "
                            "provide one of them")
        self.create_incremental_dump_load_input(
            db_type='mysql', input_name=input_name, server=server, port=port,
            user=user, password=password, database=database, schema=schema,
            tables=tables, ssh_server=ssh_server, ssh_port=ssh_port,
            ssh_username=ssh_username, ssh_password=ssh_password,
            batch_size=batch_size, one_click=one_click)

    def create_mysql_full_dump_load_replication_input(
            self, input_name, server, port, user, password, tables,
            replication_frequency, start_time, schema=None, database=None,
            ssh_server=None,  ssh_port=None, ssh_username=None,
            ssh_password=None, one_click=False):
        """
        Create a new MySQL full dump\load input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your MySQL server
        :param port: Microsoft SQL server's port
        :param user: User name to use when connecting to your
                     MySQL server
        :param password: Password to use when connecting to your
                         MySQL server
        :param tables: List of Tables to Replicate :type list or str space
                                                         separated list of table
                                                         names
        :param replication_frequency: Every how much time to replicate tables
        :param start_time: When to start running replication :type datetime
                           object that contains time attributes
        :param database: Database name to replicate
        :param schema: Schema name
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        if database is None and schema is None:
            raise TypeError("Both database and schema are None. You have to "
                            "provide one of them")
        self.create_full_dump_load_replication_input(
            db_type='mysql', input_name=input_name, port=port, tables=tables,
            replication_frequency=replication_frequency, start_time=start_time,
            server=server, user=user, password=password, schema=schema,
            ssh_server=ssh_server, ssh_port=ssh_port, ssh_username=ssh_username,
            ssh_password=ssh_password, database=database, one_click=one_click)

    def create_mysql_log_replication_input(self, input_name, server, port,
                                           user, password, tables=None,
                                           ssh_server=None,  ssh_port=None,
                                           ssh_username=None, ssh_password=None,
                                           one_click=False):
        """
        Create a new MySQL log replication input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your Microsoft SQL server

        :param port: MySQL server's port
        :param user: User name to use when connecting to your
                     MySQL server
        :param password: Password to use when connecting to your
                         MySQL server
        :param tables: List of Tables to Replicate :type list or str space
                                                         separated list of table
                                                         names
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        configuration = {
            "hostname": server,
            "port": port,
            "username": user,
            "password": password,
            "tables_to_replicate": " ".join(tables),
            "replication_type": "log_dump_load"
        }
        self.create_input(configuration=configuration, input_name=input_name,
                          input_type="MYSQL", ssh_server=ssh_server,
                          ssh_port=ssh_port, ssh_username=ssh_username,
                          ssh_password=ssh_password, one_click=one_click)

    def create_oracle_incremental_dump_load_input(self, input_name, server,
                                                  port, user, password,
                                                  database, tables,
                                                  ssh_server=None,
                                                  ssh_port=None,
                                                  ssh_username=None,
                                                  ssh_password=None,
                                                  batch_size=10000,
                                                  one_click=False):
        """
        Create a new Oracle incremental dump\load input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your Oracle server
        :param port: Oracle server's port
        :param user: User name to use when connecting to your
                     Oracle server
        :param password: Password to use when connecting to your
                         Oracle server
        :param database: Database name to replicate
        :param tables: Tables to replicate. :type dict which it's keys are the
                       tables names and the values are the update indicator
                       columns
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param batch_size: :type int
                           Bigger batch size means faster backfill, but may
                           increase the load on your Oracle server.
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        self.create_incremental_dump_load_input(
            db_type="oracle", input_name=input_name, server=server, port=port,
            user=user, password=password, database=database,
            tables=tables, ssh_server=ssh_server, ssh_port=ssh_port,
            ssh_username=ssh_username, ssh_password=ssh_password,
            batch_size=batch_size, one_click=one_click)

    def create_oracle_full_dump_load_replication_input(
            self, input_name, server, port, user, password, database,
            tables, replication_frequency=5, start_time=datetime.time(5),
            ssh_server=None,  ssh_port=None, ssh_username=None,
            ssh_password=None, one_click=False):
        """
        Create a new Oracle full dump\load input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your Oracle
        :param port: Oracle server's port
        :param user: User name to use when connecting to your
                     Oracle server
        :param password: Password to use when connecting to your
                         Oracle server
        :param database: Database name to replicate
        :param tables: List of Tables to Replicate :type list or str space
                                                         separated list of table
                                                         names
        :param replication_frequency: Every how much time to replicate tables
        :param start_time: When to start running replication :type datetime
                           object that contains time attributes
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        self.create_full_dump_load_replication_input(
            'oracle', input_name=input_name, port=port, tables=tables,
            replication_frequency=replication_frequency, start_time=start_time,
            server=server, user=user, password=password, database=database,
            ssh_server=ssh_server, ssh_port=ssh_port, ssh_username=ssh_username,
            ssh_password=ssh_password, one_click=one_click)

    def create_psql_full_dump_load_replication_input(
            self, input_name, server, port, user, password, database, schema,
            tables, replication_frequency=5, start_time=datetime.time(5),
            ssh_server=None, ssh_port=None, ssh_username=None,
            ssh_password=None, one_click=False):
        """
        Create a new PostgreSQL full dump\load input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your PostgreSQL server
        :param port: PostgreSQL server's port ]
        :param user: User name to use when connecting to your server
        :param password: Password to use when connecting to your
                         PostgreSQL server
        :param database: Database name to replicate
        :param schema: Schema name
        :param tables: List of Tables to Replicate :type list or str space
                                                         separated list of table
                                                         names
        :param replication_frequency: Every how much time to replicate tables
        :param start_time: When to start running replication :type datetime
                           object that contains time attributes
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        self.create_full_dump_load_replication_input(
            'psql', input_name=input_name, port=port, tables=tables,
            replication_frequency=replication_frequency, start_time=start_time,
            server=server, user=user, password=password, database=database,
            schema=schema, ssh_server=ssh_server, ssh_port=ssh_port,
            ssh_username=ssh_username, ssh_password=ssh_password,
            one_click=one_click)

    def create_psql_incremental_dump_load_input(self, input_name, server, port,
                                                user, password, database,
                                                schema, tables, ssh_server=None,
                                                ssh_port=None,
                                                ssh_username=None,
                                                ssh_password=None,
                                                batch_size=10000,
                                                one_click=False):
        """
        Create a new PostgreSQL incremental dump\load input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your PostgreSQL server
        :param port: Microsoft SQL server's port
        :param user: User name to use when connecting to your PostgreSQL server
        :param password: Password to use when connecting to your server
        :param database: Database name to replicate
        :param schema: Schema name
        :param tables: Tables to replicate. :type dict which it's keys are the
                       tables names and the values are the update indicator
                       columns
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param batch_size: :type int
                           Bigger batch size means faster backfill, but may
                           increase the load on your PostgreSQL server.
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        self.create_incremental_dump_load_input(
            db_type="psql", input_name=input_name, server=server, port=port,
            user=user, password=password, database=database, schema=schema,
            tables=tables, ssh_server=ssh_server, ssh_port=ssh_port,
            ssh_username=ssh_username, ssh_password=ssh_password,
            batch_size=batch_size,
            one_click=one_click)

    def create_psql_log_replication_input(self, input_name, server, port,
                                          user, password, database, schema,
                                          slot_name, tables="", ssh_server=None,
                                          ssh_port=None, ssh_username=None,
                                          ssh_password=None, one_click=False):
        """
        Create a new PostgreSQL log replication input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your PostgreSQL server
        :param port: PostgreSQL server's port
        :param user: User name to use when connecting to your PostgreSQL server
        :param password: Password to use when connecting to your PostgreSQL
                         server
        :param database: Database name to replicate
        :param schema: Schema name
        :param slot_name: Logical replication slot name
        :param tables: A space separated list of table names or list of table
                       names
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        if isinstance(tables, list):
            tables = " ".join(tables)
        configuration = {
            "username": user,
            "tables": tables,
            "replication_type": "log_dump_load",
            "hostname": server,
            "db": database,
            "slot_name": slot_name,
            "auto_map": json.dumps(tables),
            "password": password,
            "port": port,
            "schema": schema
        }
        self.create_input(configuration, input_name, "POSTGRESQL", one_click,
                          ssh_server=ssh_server, ssh_port=ssh_port,
                          ssh_username=ssh_username, ssh_password=ssh_password)

    def create_python_sdk_input(self, input_name, one_click=False):
        """
        Create a new Python SDK input
        :param input_name: Name your new input
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        """
        self.create_input(configuration={}, input_name=input_name,
                          input_type="PYTHON_SDK", one_click=one_click)

    def create_rds_heroku_psql_incremental_dump_load_input(
            self, input_name, server, port, user, password, database, schema,
            tables, batch_size=10000, ssh_server=None,  ssh_port=None,
            ssh_username=None, ssh_password=None, one_click=False):
        """
        Create a new RDS/Heroku PostgreSQL incremental dump\load input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your RDS/Heroku PostgreSQL
                       server
        :param port: RDS/Heroku PostgreSQL server's port
        :param user: User name to use when connecting to your
                     RDS/Heroku PostgreSQL server
        :param password: Password to use when connecting to your
                         RDS/Heroku PostgreSQL server
        :param database: Database name to replicate
        :param schema: Schema name
        :param tables: Tables to replicate. :type dict which it's keys are the
                       tables names and the values are the update indicator
                       columns
        :param batch_size: :type int
                           Bigger batch size means faster backfill, but may
                           increase the load on your RDS/Heroku PostgreSQL
                           server.
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        self.create_incremental_dump_load_input(
            db_type="psql", input_name=input_name, server=server, port=port,
            user=user, password=password, database=database, schema=schema,
            tables=tables, ssh_server=ssh_server, ssh_port=ssh_port,
            ssh_username=ssh_username, ssh_password=ssh_password,
            batch_size=batch_size, one_click=one_click)

    def create_rds_heroku_psql_full_dump_load_replication_input(
            self, input_name, server, port, user, password, database, schema,
            tables, replication_frequency=5, start_time=datetime.time(5),
            ssh_server=None, ssh_port=None, ssh_username=None,
            ssh_password=None, one_click=False):
        """
        Create a new RDS/Heroku PostgreSQL full dump\load input
        :param input_name: Name your new input
        :param server: Hostname or IP address of your RDS/Heroku PostgreSQL
        :param port: RDS/Heroku PostgreSQL server's port
        :param user: User name to use when connecting to your
                     RDS/Heroku PostgreSQL server
        :param password: Password to use when connecting to your
                         RDS/Heroku PostgreSQL server
        :param database: Database name to replicate
        :param schema: Schema name
        :param tables: List of Tables to Replicate :type list or str space
                                                         separated list of table
                                                         names
        :param replication_frequency: Every how much time to replicate tables
        :param start_time: When to start running replication :type datetime
                           object that contains time attributes
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        self.create_full_dump_load_replication_input(
            'psql', input_name=input_name, port=port, tables=tables,
            replication_frequency=replication_frequency, start_time=start_time,
            server=server, user=user, password=password, database=database,
            schema=schema, ssh_server=ssh_server, ssh_port=ssh_port,
            ssh_username=ssh_username, ssh_password=ssh_password,
            one_click=one_click)

    def create_s3_json_lines_input(self, input_name, key, secret, bucket,
                                   prefix='', all_files=False, one_click=False):
        """
        Create a new S3 file format - JSON input
        :param input_name: Name your new input :type str
        :param key: AWS Access Key ID :type str
        :param secret: AWS Secret Access Key :type str
        :param bucket: Bucket Name :type str
        :param prefix: File prefix (optional) :type str
        :param all_files: Select which files to import :type bool
        :param one_click: Define whether Alooma will map this input's events to
                          your target database, or you'll define your own
                          mapping. :type bool
        :return:
        """
        file_format_config = {"type": "json"}

        self.create_s3_input(input_name, key, secret, bucket, prefix, all_files,
                             file_format_config, one_click=one_click)

    def create_s3_delimited_with_headers_input(self, input_name, key, secret,
                                               bucket, prefix='',
                                               all_files=False,
                                               delimiter_character=',',
                                               file_quote_character='"',
                                               escape_character='',
                                               one_click=False):
        """
        Create a new S3 file format - Delimited with headers input
        :param input_name: Name your new input :type str
        :param key: AWS Access Key ID :type str
        :param secret: AWS Secret Access Key :type str
        :param bucket: Bucket Name :type str
        :param prefix: File prefix (optional) :type str
        :param all_files: Select which files to import :type bool
        :param delimiter_character: Delimiter character (\t for tab) :type str
        :param file_quote_character: File quote character (optional) :type str
        :param escape_character: Escape character (optional) :type str
        :param one_click: Define whether Alooma will map this input's events to
                          your target database, or you'll define your own
                          mapping. :type bool
        :return:
        """
        # Select which files to import
        file_format_config = {
            'type': 'delimited',
            'delimiter': delimiter_character,
            'quoteChar': file_quote_character,
            'escapeChar': escape_character
        }
        self.create_s3_input(input_name, key, secret, bucket, prefix, all_files,
                             file_format_config, one_click=one_click)

    def create_s3_other_input(self, input_name, key, secret, bucket, prefix='',
                              all_files=False, one_click=False):
        """
        Create a new S3 file format - Other (Delimited without header, Syslog,
        XML, other) input
        :param input_name: Name your new input :type str
        :param key: AWS Access Key ID :type str
        :param secret: AWS Secret Access Key :type str
        :param bucket: Bucket Name :type str
        :param prefix: File prefix (optional) :type str
        :param all_files: Select which files to import :type bool
        :param one_click: Define whether Alooma will map this input's events to
                          your target database, or you'll define your own
                          mapping. :type bool
        :return:
        """
        file_format_config = {'type': 'other'}

        self.create_s3_input(input_name, key, secret, bucket, prefix, all_files,
                             file_format_config, one_click=one_click)

    def create_s3_input(self, input_name, key, secret, bucket, prefix='',
                        all_files=False, file_format_config=None,
                        one_click=False):
        """
        Create a new S3 input
        :param input_name: Name your new input :type str
        :param key: AWS Access Key ID :type str
        :param secret: AWS Secret Access Key :type str
        :param bucket: Bucket Name :type str
        :param prefix: File prefix (optional) :type str
        :param all_files: Select which files to import :type bool
        :param file_format_config: The file format configs. Must contain only
                                   those keys and
        :param one_click: Define whether Alooma will map this input's events to
                          your target database, or you'll define your own
                          mapping. :type bool

        :return:
        """
        # Select which files to import
        load_files = "all" if all_files else "new"

        forbidden_file_format_config_keys = [k for k in set(file_format_config)
                                             if k not in ['type',
                                                          'delimiter',
                                                          'quoteChar',
                                                          'escapeChar']]
        if any(forbidden_file_format_config_keys):
            raise alooma_exceptions.FailedToCreateInputException(
                'file_format_config argument contains a unknown keys: '
                '"%s"' % ", ".join(file_format_config))
        if 'type' not in file_format_config.keys():
            raise alooma_exceptions.FailedToCreateInputException(
                'file_format_config must contain "type" key')
        configuration = {
            "awsAccessKeyId": key,
            "awsBucketName": bucket,
            "awsSecretAccessKey": secret,
            "filePrefix": prefix,
            "loadFiles": load_files,
            "fileFormat": json.dumps(file_format_config)
        }

        self.create_input(configuration=configuration, input_name=input_name,
                          input_type="S3", one_click=one_click)

    def create_salesforce_input(self, input_name, user, password, token,
                                objects, is_sandbox=False,
                                start_time=datetime.datetime.utcnow(),
                                daily_api_call_usage_limit=None,
                                daily_bulk_api_query_limit=None,
                                one_click=False):
        """
        Create a new Salesforce input
        :param input_name: Name your new input :type str
        :param user: Username used to connect to your Salesforce
        :param password: Password used to connect to your Salesforce
        :param token: Your Salesforce account security token
        :param objects: :type list or str: List of Salesforce objects to
                                           Replicate
        :param is_sandbox: :type bool: This is a sandbox account
        :param start_time: :type datetime: Since when to start running
                                           replication
        :param daily_api_call_usage_limit: :type int: Set a daily API call
                                                      usage limit
        :param daily_bulk_api_query_limit: :type int: Set a daily Bulk API
                                                      query limit
        :param one_click: Define whether Alooma will map this input's events to
                          your target database, or you'll define your own
                          mapping. :type bool
        :return:
        """
        configuration = {
            "custom_objects": " ".join(objects)
                              if isinstance(objects, list)
                              else objects,
            "is_sandbox": is_sandbox,
            "username": user,
            "password": password,
            "token": token,
            "start_time": start_time.strftime(DATETIME_FORMAT)
        }

        if daily_api_call_usage_limit:
            configuration['daily_api_calls'] = daily_api_call_usage_limit
        if daily_bulk_api_query_limit:
            configuration['daily_bulk_queries'] = daily_bulk_api_query_limit

        self.create_input(configuration=configuration,
                          input_name=input_name,
                          input_type='SALESFORCE',
                          one_click=one_click)

    def create_segment_input(self, input_name, one_click=False):
        """
        Create a new Segment input
        :param input_name: Name your new input
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        """
        self.create_input(configuration={}, input_name=input_name,
                          input_type="SEGMENT", one_click=one_click)

    def create_server_logs_input(self, input_name, one_click=False):
        """
        Create a new Server Logs input
        :param input_name: Name your new input
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        """
        self.create_input(configuration={}, input_name=input_name,
                          input_type="AGENT", one_click=one_click)

    def create_incremental_dump_load_input(self, db_type, input_name, server,
                                           port, user, password, tables,
                                           database=None, schema=None,
                                           ssh_server=None,  ssh_port=None,
                                           ssh_username=None, ssh_password=None,
                                           batch_size=10000, one_click=False):
        """
        Create a new Incremental dump\load input
        :param db_type: :type str. The database type
        :param input_name: :type str. Name your new input
        :param server: Hostname or IP address of your server
                       
        :param port: server's port 
        :param user: User name to use when connecting to your server 
        :param password: Password to use when connecting to your
                         server 
        :param tables: Tables to replicate. :type dict which it's keys are the
                       tables names and the values are the update indicator
                       columns
        :param database: Database name to replicate
        :param schema: Schema name
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param batch_size: :type int
                           Bigger batch size means faster backfill, but may
                           increase the load on your server.
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        if isinstance(port, str):
            port = int(port)
        configuration = {
            "db_type": db_type,
            "port": port,
            "replication_type": "incremental_load",
            "server": server,
            "user": user,
            "password": password,
            "tables": json.dumps(tables),
            "batch_size": batch_size
        }
        if schema:
            configuration['schema'] = schema
        if database:
            configuration['database'] = database

        self.create_input(configuration, input_name, "ODBC", one_click,
                          ssh_server=ssh_server,  ssh_port=ssh_port,
                          ssh_username=ssh_username, ssh_password=ssh_password)

    def create_full_dump_load_replication_input(
            self, db_type, input_name, server, port, user, password,
            tables, replication_frequency, start_time,
            database=None, schema=None, ssh_server=None,  ssh_port=None,
            ssh_username=None, ssh_password=None, one_click=False):
        """
        Create a new full dump\load input
        :param db_type: odbc input type
        :param input_name: Name your new input
        :param server: Hostname or IP address of your server 
        :param port: server's port 
        :param user: User name to use when connecting to your server 
        :param password: Password to use when connecting to your
                         server 
        :param tables: List of Tables to Replicate :type list or str space
                                                         separated list of table
                                                         names
        :param replication_frequency: Every how much time to replicate tables
        :param start_time: Since when to start running replication :type
                           datetime object that contains time attributes
        :param database: Database name to replicate
        :param schema: Schema name
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        if isinstance(tables, str):
            tables = tables.split()

        m = start_time.minute if hasattr(start_time, 'minute') else 0
        h = start_time.hour if hasattr(start_time, 'hour') else 0

        number_of_runs_per_day = 24 / replication_frequency + (
            1 if 24 % replication_frequency else 0)
        hours = [str((h + (i * replication_frequency)) % 24) for i in
                 range(number_of_runs_per_day)]

        configuration = {
            "db_type": db_type,
            "port": port,
            "replication_type": "full_dump_load",
            "tables": json.dumps(tables),
            "run_at": "%i %s * * *" % (m, ','.join(hours)),
            "server": server,
            "user": user,
            "password": password
        }
        if schema:
            configuration['schema'] = schema
        if database:
            configuration['database'] = database

        self.create_input(configuration, input_name=input_name,
                          input_type="ODBC", ssh_server=ssh_server,
                          ssh_port=ssh_port, ssh_username=ssh_username,
                          ssh_password=ssh_password, one_click=one_click)

    def create_input(self, configuration, input_name, input_type,
                     one_click=False, ssh_server=None,  ssh_port=None,
                     ssh_username=None, ssh_password=None):
        """
        Create a new input
        :param configuration: :type dict: Input's configuration object
        :param input_name: Name your new input
        :param input_type: Input's type
        :param ssh_server: (Optional) The IP or DNS of your SSH server as seen
                           from the public internet
        :param ssh_port: (Optional) The SSH port of the SSH server as seen from
                         the public internet (default port is 22)
        :param ssh_username: (Optional) The user name on the SSH server for the
                             SSH connection (the standard is alooma)
        :param ssh_password: (Optional) The password that matches the user name
                              on the SSH server
        :param one_click: :type bool. Define whether Alooma will map this
                                      input's events to your target database, or
                                      you'll define your own mapping.
        :return:
        """
        ssh_config = self._get_ssh_config(
            ssh_server=ssh_server, ssh_port=ssh_port, ssh_username=ssh_username,
            ssh_password=ssh_password)
        if ssh_config:
            configuration["ssh"] = json.dumps(ssh_config)

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

        url = self.api.rest_url + \
            'plumbing/inputs?%s' % urllib.urlencode({'autoMap': one_click})

        self.api.send_request(requests.post, url, json=post_data)

        for i in range(10):
            structure = self.get_structure()
            input_type_nodes = [
                x for x in structure['nodes']
                if x[POST_DATA_NAME] == post_data[POST_DATA_NAME]]
            if len(input_type_nodes) == len(previous_nodes) + 1:
                old_ids = set([x['id'] for x in previous_nodes])
                current_ids = set([x['id'] for x in input_type_nodes])
                try:
                    current_ids.difference(old_ids).pop()
                except KeyError:
                    pass

                return
            time.sleep(1)

        raise alooma_exceptions.FailedToCreateInputException(
                'Failed to create {type} input'
                .format(type=post_data[POST_DATA_TYPE]))

    def delete_input(self, input_name, input_id=None, delete_event_types=False):
        """
        Delete an input by it's name
        :param input_name: The input name you want delete
        :param input_id: The input's ID (Optional)
        :param delete_event_types: :type bool: In case you want to delete
                                               input's event types then set to
                                               True. (Note; If you have multiple
                                               inputs that mapped to the same
                                               event type might me deleted also,
                                               in case this input would be the
                                               most common one)
        """
        if input_id is None:
            input_id = next(input_config['id'] for input_config in
                            self.get_input_config(input_name=input_name)
                            if input_config['name'] == input_name)
        url = "{rest_url}plumbing/nodes/remove/{input_id}".format(
                rest_url=self.api.rest_url, input_id=input_id)
        self.api.send_request(requests.post, url)

        if delete_event_types:
            event_types_main_stats_input_label = self.api.mapper.\
                get_event_type_main_stats_input_label()
            for k, v in event_types_main_stats_input_label.iteritems():
                if v == input_name:
                    self.api.mapper.delete_event_type(k)

    def get_structure(self):
        """
        Returns a representation of all the inputs, outputs,
        and on-stream processors currently configured in the system
        :return: A dict representing the structure of the system
        """
        url_get = self.api.rest_url + 'plumbing/?resolution=1min'
        response = self.api.send_request(requests.get, url_get)
        return self.api.parse_response_to_json(response)

    def get_input_config(self, input_name=None, input_type=None, input_id=None):
        """
        Get a list of all the input nodes with their configurations.
        :param input_name: Filter by name (accepts Regex)
        :param input_type: Filter by type (e.g. "mysql")
        :param input_id: Filter by node ID
        :return: A list of all the inputs in the system, along
        with metadata and configurations
        """

        nodes = self._get_inputs_nodes()
        if input_name:
            input_id = self._get_input_id_by_input_name(input_name=input_name)
        if input_id:
            nodes = [node for node in nodes if node['id'] == input_id]
        elif input_type:
            nodes = [node for node in nodes
                     if node['type'].lower() == input_type.lower()]
        else:
            raise TypeError('get_input_config() takes exactly 1 argument '
                            '(0 given)')
        return nodes

    def delete_all_inputs(self, delete_event_types=False):
        """
        Will delete all your inputs
        :param delete_event_types: If sat to True then after removing all of
        your inputs then it will remove all you event types
        """
        plumbing = self.get_structure()
        for node in plumbing["nodes"]:
            if node["category"] == "INPUT" \
                    and node["type"] not in ["RESTREAM", "AGENT"]:
                self.delete_input(node["id"])

        if delete_event_types:
            self.api.mapper.delete_all_event_types()

    def get_input_sleep_time(self, input_name=None, input_id=None):
        """
        Get the sleep time for an input
        :param input_name: Filter by name (accepts Regex)
        :param input_id: The input's ID (Optional)
        :return: sleep time of the input with ID input_id
        """
        if input_name:
            input_id = self._get_input_id_by_input_name(input_name=input_name)
        if not input_id:
            raise TypeError('get_input_sleep_time() takes exactly 1 argument '
                            '(0 given)')
        url = self.api.rest_url + 'inputSleepTime/%s' % input_id
        res = self.api.send_request(requests.get, url)
        return float(json.loads(res.content).get('inputSleepTime'))

    def set_input_sleep_time(self, input_name=None, input_id=None,
                             sleep_time=5):
        """
        Update input's sleep time
        :param input_name: Filter by name (accepts Regex)
        :param input_id: The input's ID (Optional)
        :param sleep_time: new sleep time to set for input with ID input_id
        :return:           result of the REST request
        """
        if input_name:
            input_id = self._get_input_id_by_input_name(input_name=input_name)
        if not input_id:
            raise TypeError('set_input_sleep_time() takes exactly 1 argument '
                            '(0 given)')
        url = self.api.rest_url + 'inputSleepTime/%s' % input_id
        res = self.api.send_request(requests.put, url, data=str(sleep_time))
        return res

    def get_input_structure(self, input_name=None, input_id=None):
        if input_name:
            input_id = self._get_input_id_by_input_name(input_name=input_name)
        if not input_id:
            raise TypeError('get_input_structure() takes exactly 1 argument '
                            '(0 given)')
        url = "%splumbing/nodes/%s" % (self.api.rest_url, input_id)
        res = self.api.send_request(requests.get, url)
        return json.loads(res.content)

    def get_input_types(self):
        url = self.api.rest_url + "plumbing/types"
        res = self.api.send_request(requests.get, url).json()
        return res["INPUT"]

    def get_token(self, input_name, input_type):
        """
        Get token string for an input by it's name and type
        :param input_name: Name your new input
        :param input_type: The type ov your new input
        :return: :type str: Token
        """
        url = self.api.rest_url + 'token?inputLabel=%s&inputType=%s' % (
            input_name, input_type.upper())
        token = self.api.send_request(requests.get, url)
        return token.json()["token"]

    def _get_input_node_id(self, name):
        return self._get_node_by("name", name)["id"]

    def _get_node_by(self, field, value):
        """
        Get the node by (id, name, type, etc...)
        e.g. _get_node_by("type", "RESTREAM") ->
        :param field: the field to look the node by it
        :param value: tha value of the field
        :return: first node that found, if no node found for this case return
        None
        """
        return next(node for node in self.get_structure()
                    if node[field].lower() == value.lower())

    def _get_input_id_by_input_name(self, input_name):
        """
        Returns input's ID by it's name
        :param input_name:
        :return:
        """
        nodes = self._get_inputs_nodes()
        regex = re.compile(input_name)
        input_id = next(node['id'] for node in nodes
                        if regex.match(node['name']))
        return input_id

    def _get_inputs_nodes(self):
        """
        Get all inputs nodes that are alive
        :return: :type list of dicts: list of inputs structure
        """
        nodes = [node for node in self.get_structure()['nodes']
                 if node['category'] == 'INPUT' and node['type'] not in
                 ['RESTREAM', 'AGENT'] and not node['deleted']]
        return nodes

    @staticmethod
    def get_public_ssh_key():
        """
        Get public SSH key, uses for adding input\output with SSH and this is
        the required ssh key that need to be added to your access list
        :return: :type str: SSH Public key string
        """
        return "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC+t5OKwGcUGYRdDAC8ovblV" \
               "/10AoBfuI/nmkxgRx0J+M3tIdTdxW0Layqb6Xtz8PMsxy1uhM+Rw6cXhU/FQW" \
               "bOr7MB5hJUqXY5OI4NVtI+cc2diCyYUjgCIb7dBSKoyZecJqp3bQuekuZT/Ow" \
               "Z40vLc42g6cUV01b5loV9pU9DvRl6zZXHyrE7fssJ90q2lhvuBjltU7g543bU" \
               "klkYtzwqzYpcynNyrCBSWd85aa/3cVPdiugk7hV4nuUk3mVEX/l4GDIsTkLIR" \
               "zHUHDwt5aWGzhpwdle9D/fxshCbp5nkcg1arSdTveyM/PdJJEHh65986tgprb" \
               "I0Lz+geqYmASgF deploy@alooma.io"

    @staticmethod
    def _get_ssh_config(ssh_server, ssh_port, ssh_username, ssh_password=None):
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
