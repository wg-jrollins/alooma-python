import requests

from consts import METRICS_LIST


class _Metrics(object):
    def __init__(self, api):
        self.api = api

    def get_metrics_by_names(self, metric_names, minutes):
        if type(metric_names) != list and type(metric_names) == str:
            metric_names = [metric_names]
        elif type(metric_names) != str and type(metric_names) != list:
            raise Exception("metric_names can be only from type `str` or "
                            "`list`")
        for metric_name in metric_names:
            if metric_name not in METRICS_LIST:
                raise Exception(
                        "Metrics '{name}' not exists, please use one or more "
                        "of those: {metrics}".format(
                                name=metric_names, metrics=METRICS_LIST))

        metrics_string = ",".join(metric_names)
        url = self.api.rest_url + 'metrics?metrics=%s&from=-%dmin&resolution=%dmin' \
                                     '' % (metrics_string, minutes, minutes)
        res = self.api.send_request(requests.get, url)

        response = self.api.parse_response_to_json(res)
        return response

    def get_incoming_queue_metric(self, minutes):
        response = self.get_metrics_by_names("EVENTS_IN_PIPELINE", minutes)
        incoming = non_empty_datapoint_values(response)
        if incoming:
            return max(incoming)
        else:
            return 0

    def get_outputs_metrics(self, minutes):
        """
        Returns the number of events erred / unmapped / discarded / loaded in
        the last X minutes
        :param minutes: number of minutes to check
        """
        response = self.get_metrics_by_names(['UNMAPPED_EVENTS',
                                              'IGNORED_EVENTS',
                                              'ERROR_EVENTS',
                                              'LOADED_EVENTS_RATE'],
                                             minutes)
        return tuple([sum(non_empty_datapoint_values([r])) for r in response])

    def get_restream_queue_metrics(self, minutes=120):
        """
        Returns the current number of events stored in the Restream Queue.
        :param minutes: The number of minutes to go back in time to find the
        number of events in the queue if the metric isn't available now for
        some reason
        :return: an int representing the amount of events in the Restream Queue
        """
        response = self.get_metrics_by_names("EVENTS_IN_TRANSIT", minutes)
        return non_empty_datapoint_values(response)[0]

    def get_incoming_events_count(self, minutes=120):
        """
        Returns the number of events waiting in the Incoming Queue to be
        processed by Alooma.
        :param minutes: The number of minutes to go back in time to find the
        number of events in the queue if the metric isn't available now for
        some reason
        :return: an int representing the amount of events in the Incoming Queue
        """
        response = self.get_metrics_by_names("INCOMING_EVENTS", minutes)
        return sum(non_empty_datapoint_values(response))

    def get_average_event_size(self, minutes):
        response = self.get_metrics_by_names("EVENT_SIZE_AVG", minutes)
        values = non_empty_datapoint_values(response)
        if not values:
            return 0

        return sum(values) / len(values)

    def get_max_latency(self, minutes):
        try:
            response = self.get_metrics_by_names("LATENCY_MAX", minutes)
            latencies = non_empty_datapoint_values(response)
            if latencies:
                return max(latencies) / 1000
            else:
                return 0
        except Exception as e:
            raise Exception("Failed to get max latency, returning 0. "
                            "Reason: %s", e)


def non_empty_datapoint_values(data):
    """
    From a graphite like response, return the values of the non-empty datapoints
    :param data: A graphite response containing metrics
    """
    if data:
        return [t[0] for t in data[0]['datapoints'] if t[0]]
    return []

SUBMODULE_CLASS = _Metrics
