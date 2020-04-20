import datetime
import logging

import bonobo
from bonobo.config import use, Configurable, Option, Service, use_context

from . import db
from .messages_fetcher import MessagesFetcher
from .channel_grouper import ChannelGrouper
from .date_utils import date_range, nworking_days_before


class RecentlyActiveChannelSource(Configurable):

    message_count_database = Service("message_count")
    date = Option(required=True, positional=True)

    def __call__(self, message_count_database):
        all_channels = set()
        for day in date_range(nworking_days_before(self.date, 3), self.date):
            channels = message_count_database.get_channels_for_day(day)
            all_channels.update(channels)
        yield from all_channels


@use("channels")
def get_channels(channels):
    for channel in channels.all():
        if channel.get("is_member") or channel.get("is_im"):
            yield channel["id"]


def set_message_count(channel, messages):
    number_messages = len(messages)
    if number_messages:
        yield (channel, len(messages))


def process_message(channel, message):
    yield {"channel": channel}


@use_context
class MessageCountWriter(Configurable):

    date = Option(required=True, positional=True)
    message_count_database = Service("message_count")

    def __call__(self, _, channel, count, *, message_count_database):
        message_count_database.set_day_channel(self.date, channel, count)


def get_message_count_graph(day, quick):
    graph = bonobo.Graph()
    graph.add_chain(
        RecentlyActiveChannelSource(day) if quick else get_channels,
        MessagesFetcher(day, day + datetime.timedelta(days=1)),
        process_message,
        ChannelGrouper(),
        set_message_count,
        MessageCountWriter(day)
    )
    return graph


def get_message_count_services(base_services):
    database = base_services["database"]
    channels = db.Channels(database)
    message_count = db.MessageCount(database)
    services = {
        **base_services,
        "channels": channels,
        "message_count": message_count
    }
    return services


def update_message_count_quick(date, base_services):
    services = get_message_count_services(base_services)
    logging.info(
        f"Fetching raw messages for date {date.isoformat()} in quick mode")
    graph = get_message_count_graph(date, True)
    bonobo.run(graph, services=services)


def update_message_count(start_date, end_date, base_services):
    database = base_services["database"]
    status_db = db.Status(database)
    services = get_message_count_services(base_services)

    for date in date_range(start_date, end_date):
        if not status_db.is_message_count_complete(date):
            logging.info(f"Fetching raw messages for {date.isoformat()}")
            graph = get_message_count_graph(date, False)
            bonobo.run(graph, services=services)
            if date < datetime.date.today():
                status_db.set_message_count_complete(date)
        else:
            logging.info(f"Date {date.isoformat()} is complete. Skipping.")
