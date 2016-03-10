import requests
import time

RESTREAM_QUEUE_TYPE_NAME = "RESTREAM"


class _Restream(object):
    def __init__(self, api):
        self.__api = api
        self.__send_request = api._Alooma__send_request

    def clean_restream_queue(self):
        event_types = self.__api.mapper.get_event_types()
        for event_type in event_types:
            self.__api.mapper.discard_event_type(event_type["name"])

        self.start_restream()
        queue_depth = self.get_restream_queue_size()
        while queue_depth != 0:
            queue_depth = self.get_restream_queue_size()
            time.sleep(1)

    def start_restream(self):
        restream_node = self.__api._get_node_by(
                'type', RESTREAM_QUEUE_TYPE_NAME)

        if restream_node:
            restream_id = restream_node["id"]
            url = self.__api.rest_url + "plumbing/nodes/{restream_id}".format(
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
            self.__send_request(requests.put, url,
                                json=restream_click_button_json)
        else:
            raise Exception("Could not find '{restream_type}' type".format(
                    restream_type=RESTREAM_QUEUE_TYPE_NAME))

    def get_restream_queue_size(self):
        restream_node = self.__api._get_node_by(
                "type", RESTREAM_QUEUE_TYPE_NAME)
        return restream_node["stats"]["availbleForRestream"]