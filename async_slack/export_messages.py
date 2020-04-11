import json
from pathlib import Path
import os
import datetime
from functools import lru_cache
import logging

import dpath.util

from slacker import Slacker

from tenacity import stop_after_attempt, wait_exponential, retry, after_log

import bonobo
from bonobo.config import Configurable, Service, use, Option

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

SLACK_TOKEN = os.environ["SLACK_TOKEN"]

@lru_cache(maxsize=1)
def _get_client():
    return Slacker(SLACK_TOKEN)


class ChannelsJsonDatabase:

    def __init__(self, input_path):
        self._input_path = input_path
        self._data = self._read_input()

    def _read_input(self):
        with self._input_path.open() as rb:
            lines = [json.loads(line) for line in rb]
        return lines

    def all(self):
        yield from self._data.copy()


@use("channels_database")
def get_channels(channels_database):
    for channel in channels_database.all():
        if channel.get("is_member"):
            yield channel["id"]


@retry(
    wait=wait_exponential(multiplier=1, min=60, max=1800), 
    reraise=True, 
    stop=stop_after_attempt(10), 
    after=after_log(logger, logging.INFO)
)
def get_history(channel_id, latest, oldest):
    client = _get_client()
    return client.conversations.history(
        channel_id, 
        oldest=str(oldest.timestamp()),
        latest=str(latest.timestamp())
    ).body


class MessagesFetcher(Configurable):
    start_date = Option(positional=True, required=True)
    end_date = Option(positional=True, required=True)

    def __call__(self, channel_id):
        client = _get_client()
        oldest = datetime.datetime.combine(self.start_date, datetime.time(0, 0))
        latest = datetime.datetime.combine(self.end_date, datetime.time(0, 0))
        response = get_history(channel_id, latest, oldest)

        for message in response["messages"]:
            yield (channel_id, message)
        
        while response["has_more"]:
            next_latest = response["messages"][-1]["ts"]
            response = get_history(channel_id, next_latest, oldest)
            for message in response["messages"]:
                yield (channel_id, message)


def safe_get(obj, path):
    try:
        return dpath.util.get(obj, path)
    except KeyError:
        return None


def process_channel_message(channel_id, message):
    if message.get("subtype") != "bot_message":
        print(channel_id, message)
        to_keep = {
            "blocks": "/blocks",
            "ts": "/ts",
            "thread_ts": "/thread_ts",
            "user": "/user",
        }
        new_message = {k: safe_get(message, path) for k, path in to_keep.items()}
        new_message["channel"] = channel_id
        yield new_message


def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        get_channels,
        MessagesFetcher(datetime.date(2020, 4, 9), datetime.date(2020, 4, 10)),
        process_channel_message,
        bonobo.LdjsonWriter(path="messages.json", mode="w")
    )
    return graph


def get_services(**optons):
    return {
        "channels_database": ChannelsJsonDatabase(Path("/project/channels.json"))
    }


if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(get_graph(**options), services=get_services(**options))