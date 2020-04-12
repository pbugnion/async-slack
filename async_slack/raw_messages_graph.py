import json
from pathlib import Path
import os
import datetime
from functools import lru_cache
import logging

import bonobo
from bonobo.config import Configurable, Service, use, Option

from . import slack
from . import db
from .dict_utils import map_dictionary


@use("channels")
def get_channels(channels):
    for channel in channels.all():
        if channel.get("is_member"):
            yield channel["id"]


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

    def __call__(self, channel_id):
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


def process_channel_message(channel_id, message):
    if message.get("subtype") != "bot_message":
        to_keep = {
            "blocks": "/blocks",
            "ts": "/ts",
            "thread_ts": "/thread_ts",
            "user": "/user",
        }
        new_message = map_dictionary(to_keep, message)
        new_message["channel"] = channel_id
        yield new_message


def get_raw_messages_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        get_channels,
        MessagesFetcher(datetime.date(2020, 4, 9), datetime.date(2020, 4, 10)),
        process_channel_message,
        db.JsonRawMessagesWriter()
    )
    return graph


def get_raw_messages_services(base_services, **options):
    database = base_services["database"]
    return {
        **base_services,
        "channels": db.Channels(database)
    }
