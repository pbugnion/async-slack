import datetime

import bonobo
from bonobo.config import use, Configurable, Option

from . import db
from .date_utils import date_range


class DateRangeNode(Configurable):

    start_date = Option(positional=True, required=True)
    end_date = Option(positional=True, required=True)

    def __call__(self):
        for date in date_range(self.start_date, self.end_date):
            yield date


def add_user_to_message(message, users):
    message = message.copy()
    try:
        user_id = message["user"]
        user = users.for_id(user_id)
    except KeyError:
        pass
    else:
        message["user_name"] = user["name"]
        message["user_full_name"] = user["real_name"]
    return message


@use("users")
def add_user(message, users):
    updated_message = add_user_to_message(message, users)
    try:
        thread = updated_message["thread"]
        updated_message["thread"] = [
            add_user_to_message(reply, users) for reply in thread
        ]
    except KeyError:
        pass
    yield updated_message


@use("channels")
def add_channel(message, channels):
    message = message.copy()
    try:
        channel_id = message["channel"]
        channel = channels.for_id(channel_id)
    except KeyError:
        pass
    else:
        message["channel_name"] = channel["name"]
    yield message


def get_enriched_messages_graph(start_date, end_date, **options):
    graph = bonobo.Graph()
    graph.add_chain(
        DateRangeNode(start_date, end_date),
        db.JsonRawThreadsReader(),
        add_user,
        add_channel,
        db.JsonEnrichedMessagesWriter()
    )
    return graph


def get_enriched_messages_services(base_services, **options):
    database = base_services["database"]
    users = db.Users(database)
    channels = db.Channels(database)
    return {
        **base_services,
        "users": users,
        "channels": channels
    }
    
