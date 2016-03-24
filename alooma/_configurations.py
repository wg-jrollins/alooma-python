import requests

import alooma

DEFAULT_SETTINGS_EMAIL_NOTIFICATIONS = {
    "digestInfo": True,
    "digestWarning": True,
    "digestError": True,
    "digestFrequency": "DAILY",
    "recipientsChanged": False,
    "recipients": []
}


class _Configurations(object):
    def __init__(self, api):
        self.__api = api

    def get_config(self):
        """
        Exports the entire system configuration in dict format.
        This is also used periodically by Alooma for backup purposes,
        :return: a dict representation of the system configuration
        """
        url_get = self.__api._rest_url + 'config/export'
        response = self.__api._send_request(requests.get, url=url_get)
        config_export = alooma.parse_response_to_json(response)
        return config_export

    def clean_system(self):
        """
        USE CAREFULLY! This function deletes the state from the machine
        and returns it to its factory settings. It usually should not
        be used in production machines!
        """
        self.__api.code_engine.set_code_to_default()
        self.__api.restream.clean_restream_queue()
        self.__api.structure.remove_all_inputs()
        self.__api.mapper.delete_all_event_types()
        self.__api.notifications.set_settings_email_notifications(
                DEFAULT_SETTINGS_EMAIL_NOTIFICATIONS)
        self.delete_s3_retention()

    def get_users(self):
        url = self.__api._rest_url + 'users/'

        res = self.__api._send_request(requests.get, url)
        return alooma.parse_response_to_json(res)

    def get_settings(self):
        url = self.__api._rest_url + 'settings/'

        res = self.__api._send_request(requests.get, url)
        return alooma.parse_response_to_json(res)

    def delete_s3_retention(self):
        url = self.__api._rest_url + "settings/s3-retention"
        self.__api._send_request(requests.delete, url)