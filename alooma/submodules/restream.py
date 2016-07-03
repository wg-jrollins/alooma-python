import requests

from consts import RESTREAM_QUEUE_TYPE_NAME


class _Restream(object):
    def __init__(self, api):
        self.api = api

    # Commented this out as it can easily make users destroy the state of their
    # system - Yuval
    # def clean_restream_queue(self, restore_mapping=False):
    #     """
    #     Clears all the events in the restream queue by discarding all the
    #     event types in the system and initiating a restream
    #     """
    #     original_event_types = {}
    #     event_types = self.api.mapper.get_event_types()
    #     for event_type_name, event_type in event_types.iteritems():
    #         if restore_mapping and event_type['state'] != 'UNMAPPED':
    #             original_event_types[event_type_name] = event_type
    #         self.api.mapper.discard_event_type(event_type_name)
    #
    #     self.start_restream()
    #     queue_depth = self.get_restream_queue_size()
    #     while queue_depth != 0:
    #         queue_depth = self.get_restream_queue_size()
    #         time.sleep(1)
    #
    #     if restore_mapping:
    #         for event_type_name, mapping in original_event_types.iteritems():
    #             self.api.mapper.set_mapping(event_type_name, mapping)

    def start_restream(self):
        """
        Starts a Restream, streaming data from the Restream Queue
        to the pipeline for processing
        """
        restream_node = self.api._get_node_by(
                'type', RESTREAM_QUEUE_TYPE_NAME)

        if restream_node:
            restream_id = restream_node["id"]
            url = self.api.rest_url + "plumbing/nodes/{restream_id}".format(
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
            self.api.send_request(requests.put, url,
                                json=restream_click_button_json)
        else:
            raise Exception("Could not find '{restream_type}' type".format(
                    restream_type=RESTREAM_QUEUE_TYPE_NAME))

    def get_restream_queue_size(self):
        """
        Returns the number of events currently held in the Restream Queue
        :return: an int representing the number of events in the queue
        """
        restream_node = self.api._get_node_by(
                "type", RESTREAM_QUEUE_TYPE_NAME)
        return restream_node["stats"]["availbleForRestream"]

SUBMODULE_CLASS = _Restream
