import requests


class _Notifications(object):
    def __init__(self, api):
        self.api = api

    def get_notifications(self, epoch_time):
        url = self.api._rest_url + "notifications?from={epoch_time}". \
            format(epoch_time=epoch_time)
        res = self.api._send_request(requests.get, url)
        return self.api._parse_response_to_json(res)

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

    def set_settings_email_notifications(self, email_settings_json):
        url = self.api._rest_url + "settings/email-notifications"
        self.api._send_request(requests.post, url, json=email_settings_json)

SUBMODULE_CLASS = _Notifications
