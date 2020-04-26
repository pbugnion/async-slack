import datetime

from bonobo.config import Configurable, Option, Service
from . import slack


@slack.api_retry
def get_history(client, channel_id, latest, oldest):
    return client.conversations.history(
        channel_id,
        oldest=oldest,
        latest=latest,
    ).body


class MessagesFetcher(Configurable):
    start_date = Option(positional=True, required=True)
    end_date = Option(positional=True, required=True)
    slack = Service("slack")

    def __call__(self, channel_id, *, slack):  # pylint: disable=arguments-differ
        midnight = datetime.time(0, 0)
        oldest = str(datetime.datetime.combine(self.start_date, midnight).timestamp())
        latest = str(datetime.datetime.combine(self.end_date, midnight).timestamp())
        response = get_history(slack.client, channel_id, latest, oldest)

        for message in response["messages"]:
            yield (channel_id, message)

        while response["has_more"]:
            next_latest = response["messages"][-1]["ts"]
            response = get_history(slack.client, channel_id, next_latest, oldest)
            for message in response["messages"]:
                yield (channel_id, message)
