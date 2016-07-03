import requests

from alooma import consts


class Configurations(object):
    def __init__(self, api):
        self.api = api

    def get_config(self):
        """
        Exports the entire system configuration in dict format.
        This is also used periodically by Alooma for backup purposes,
        :return: a dict representation of the system configuration
        """
        url_get = self.api.rest_url + 'config/export'
        response = self.api.send_request(requests.get, url=url_get)
        config_export = self.api.parse_response_to_json(response)
        return config_export

    def clean_system(self):
        """
        USE CAREFULLY! This function deletes the state from the machine
        and returns it to its factory settings. It usually should not
        be used in production machines!
        """
        self.api.code_engine.deploy_default_code()
        self.api.restream.clean_restream_queue()
        self.api.structure.remove_all_inputs()
        self.api.mapper.delete_all_event_types()
        self.api.notifications.set_settings_email_notifications(
                consts.DEFAULT_SETTINGS_EMAIL_NOTIFICATIONS)
        self.delete_s3_retention()

    def get_users(self):
        url = self.api.rest_url + 'users/'

        res = self.api.send_request(requests.get, url)
        return self.api.parse_response_to_json(res)

    def get_settings(self):
        url = self.api.rest_url + 'settings/'

        res = self.api.send_request(requests.get, url)
        return self.api.parse_response_to_json(res)

    def delete_s3_retention(self):
        url = self.api.rest_url + "settings/s3-retention"
        self.api.send_request(requests.delete, url)
