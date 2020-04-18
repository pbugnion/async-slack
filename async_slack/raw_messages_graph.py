import datetime
import logging

import bonobo
from bonobo.config import use

from . import db
from .dict_utils import map_dictionary
from .messages_fetcher import MessagesFetcher


@use("channels")
def get_channels(channels):
    for channel in channels.all():
        if channel.get("is_member") or channel.get("is_im"):
            yield channel["id"]



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


def get_raw_messages_graph(day, **options):
    graph = bonobo.Graph()
    graph.add_chain(
        get_channels,
        MessagesFetcher(day, day + datetime.timedelta(days=1)),
        process_channel_message,
        db.JsonRawMessagesWriter(date=day)
    )
    return graph


def get_raw_messages_services(base_services, **options):
    database = base_services["database"]
    return {
        **base_services,
        "channels": db.Channels(database)
    }


def update_raw_messages(start_date, end_date, base_services):
    database = base_services["database"]
    status_db = db.Status(database)
    services = get_raw_messages_services(base_services)
    for ndays in range((end_date - start_date).days):
        date = start_date + datetime.timedelta(days=ndays)
        if not status_db.is_raw_messages_complete(date):
            logging.info(f"Fetching raw messages for {date.isoformat()}")
            graph = get_raw_messages_graph(date)
            bonobo.run(graph, services=services)
            if date < datetime.date.today():
                status_db.set_raw_messages_complete(date)
        else:
            logging.info(f"Date {date.isoformat()} is complete. Skipping.")
