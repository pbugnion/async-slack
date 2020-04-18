import datetime

from bonobo.config import Configurable, Option
from . import slack


@slack.api_retry
def get_history(channel_id, latest, oldest):
    client = slack.get_client()
    return client.conversations.history(
        channel_id,
        oldest=str(oldest.timestamp()),
        latest=str(latest.timestamp())
    ).body


class MessagesFetcher(Configurable):
    start_date = Option(positional=True, required=True)
    end_date = Option(positional=True, required=True)

    def __call__(self, channel_id):  # pylint: disable=arguments-differ
        midnight = datetime.time(0, 0)
        oldest = datetime.datetime.combine(self.start_date, midnight)
        latest = datetime.datetime.combine(self.end_date, midnight)
        response = get_history(channel_id, latest, oldest)

        for message in response["messages"]:
            yield (channel_id, message)

        while response["has_more"]:
            next_latest = response["messages"][-1]["ts"]
            response = get_history(channel_id, next_latest, oldest)
            for message in response["messages"]:
                yield (channel_id, message)
