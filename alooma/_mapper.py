import urllib

import requests

import alooma

MAPPING_MODES = ['AUTO_MAP', 'STRICT', 'FLEXIBLE']


class _Mapper(object):
    def __init__(self, api):
        self.__api = api
        self.__send_request = api._Alooma__send_request

    def get_mapping_mode(self):
        """
        Returns the default mapping mode currently set in the system.
        The mapping mode should be one of the values in
        alooma._mapper.MAPPING_MODES
        """
        url = self.__api._rest_url + 'mapping-mode'
        res = self.__send_request(requests.get, url)
        return res.content.replace('"', '')

    def set_mapping_mode(self, flexible):
        """
        Sets the default mapping mode in the system. The mapping
        mode should be one of the values in alooma._mapper.MAPPING_MODES
        """
        url = self.__api._rest_url + 'mapping-mode'
        res = requests.post(url, json='FLEXIBLE' if flexible else 'STRICT',
                            **self.__api.requests_params)
        return res

    def get_mapping(self, event_type):
        event_type = self.__api.get_event_type(event_type)
        mapping = self.__api.remove_stats(event_type)
        return mapping

    def map_field(self, schema, field_path, column_name, field_type, non_null,
                  **type_attributes):
        """
        :param  schema: this is the mapping json
        :param  field_path: this would use us to find the keys
        :param  field_type: the field type (VARCHAR, INT, FLOAT...)
        :param  type_attributes:    some field type need different attributes,
                                    for example:
                                        1. INT doesn't need any attributes.
                                        2. VARCHAR need the max column length
        :param column_name: self descriptive
        :param non_null: self descriptive
        :return: new mapping dict with new argument
        """

        field = self.__api.find_field_name(schema, field_path, True)
        self.__api.set_mapping_for_field(field, column_name, field_type,
                                         non_null, **type_attributes)

    def delete_all_event_types(self):
        res = self.get_event_types()
        for event_type in res:
            self.delete_event_type(event_type["name"])

    def delete_event_type(self, event_type):
        event_type = urllib.parse.quote(event_type, safe='')
        url = self.__api._rest_url + 'event-types/{event_type}' \
            .format(event_type=event_type)

        self.__send_request(requests.delete, url)

    def get_event_types(self):
        """
        Returns a dict representation of all the event-types which
        exist in the system
        """
        url = self.__api._rest_url + 'event-types'
        res = self.__send_request(requests.get, url)
        return alooma.parse_response_to_json(res)

    def get_event_type(self, event_type):
        """
        Returns a dict representation of the requested event-type's
        mapping and metadata if it exists
        :param event_type:  The name of the event type
        :return: A dict representation of the event-type's data
        """
        event_type = urllib.parse.quote(event_type, safe='')
        url = self.__api._rest_url + 'event-types/' + event_type

        res = self.__send_request(requests.get, url)
        return alooma.parse_response_to_json(res)

    def discard_event_type(self, event_type):
        """
        Discards an event type in the Mapper, stopping it from being loaded
        to Redshift or being stored in the Restream Queue
        :param event_type: A str representing an existing event type
        """
        event_type_json = {
            "name": event_type,
            "mapping": {
                "isDiscarded": True,
                "tableName": ""
            },
            "fields": [], "mappingMode": "STRICT"
        }
        self.set_mapping(event_type_json, event_type)

    def discard_field(self, mapping, field_path):
        """
        :param mapping: this is the mapping json
        :param field_path:  this would use us to find the keys

        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  the "c" field_path == a.b.c
        :return: new mapping JSON that the last argument would be discarded
        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  field_path == "a.b.c"
                        3.  then the mapping would be as the old but the "c"
                            field that would be discarded
        """

        field = self.find_field_name(mapping, field_path)
        if field:
            field["mapping"]["isDiscarded"] = True
            field["mapping"]["columnName"] = ""
            field["mapping"]["columnType"] = None

    def unmap_field(self, mapping, field_path):
        """
        :param mapping: this is the mapping json
        :param field_path:  this would use us to find the keys

        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  the "c" field_path == a.b.c
        :return: new mapping JSON that the last argument would be removed
        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  field_path == "a.b.c"
                        3.  then the mapping would be as the old but the "c"
                            field that would be removed -> {"a":1, b:{}}
        """
        field = self.find_field_name(mapping, field_path)
        if field:
            mapping["fields"].remove(field)

    def find_field_name(self, schema, field_path, add_if_missing=False):
        """
        :param schema:  this is the dict that this method run over ot
                        recursively
        :param field_path: this would use us to find the keys
        :param add_if_missing: add the field if missing
        :return:    the field that we wanna find and to do on it some changes.
                    if the field is not found then raise exception
        """

        fields_list = field_path.split('.', 1)
        if not fields_list:
            return None

        current_field = fields_list[0]
        remaining_path = fields_list[1:]

        field = next((field for field in schema["fields"]
                      if field['fieldName'] == current_field), None)
        if field:
            if not remaining_path:
                return field
            return self.__api.find_field_name(field, remaining_path[0])
        elif add_if_missing:
            parent_field = schema
            for field in fields_list:
                parent_field = self.__api.add_field(parent_field, field)
            return parent_field
        else:
            # raise this if the field is not found,
            # not standing with the case ->
            # field["fieldName"] == field_to_find
            raise Exception("Could not find field path")

    def set_mapping(self, mapping, event_type):
        event_type = urllib.parse.quote(event_type, safe='')
        url = self.__api._rest_url + 'event-types/{event_type}/mapping'.format(
                event_type=event_type)
        res = self.__send_request(requests.post, url, json=mapping)
        return res

    @staticmethod
    def set_mapping_for_field(field, column_name,
                              field_type, non_null, **type_attributes):
        column_type = {"type": field_type, "nonNull": non_null}
        column_type.update(type_attributes)
        field["mapping"] = {
            "columnName": column_name,
            "columnType": column_type,
            "isDiscarded": False
        }

    @staticmethod
    def add_field(parent_field, field_name):
        field = {
            "fieldName": field_name,
            "fields": [],
            "mapping": None
        }
        parent_field["fields"].append(field)
        return field
