import datetime
import logging

import bonobo  # type: ignore
from bonobo.config import Configurable, ContextProcessor, use_raw_input  # type: ignore
from bonobo.util import ValueHolder  # type: ignore

from . import db
from . import slack
from .messages_fetcher import MessagesFetcher
from .dict_utils import map_dictionary
from .date_utils import date_range, nworking_days_before


class ChannelExtractor(Configurable):
    """ Get unique channel names from the raw messages. """

    @ContextProcessor
    def acc(self, context):
        channels = yield ValueHolder(set())
        for channel in channels.get():
            context.send(channel)

    @use_raw_input
    def __call__(self, channels, message):  # pylint: disable=arguments-differ
        channel_id = message.get("channel")
        if channel_id:
            channels.add(channel_id)


@slack.api_retry
def get_replies(channel_id, thread_ts, oldest):
    client = slack.get_client()
    return client.conversations.replies(
        channel_id,
        thread_ts,
        oldest=oldest
    ).body


def fetch_thread_for_message(channel_id, thread_ts):
    response = get_replies(channel_id, thread_ts, thread_ts)

    for message in response["messages"]:
        if message["ts"] != thread_ts:  # don't return the original message
            yield message

    while response["has_more"]:
        next_oldest = response["messages"][-1]["ts"]
        response = get_replies(channel_id, thread_ts, next_oldest)
        for message in response["messages"]:
            yield message


def process_message_in_thread(message):
    to_keep = {
        "blocks": "/blocks",
        "ts": "/ts",
        "user": "/user",
    }
    return map_dictionary(to_keep, message)


def remove_invalid_messages(channel_id, message):
    if message.get("subtype") != "bot_message" and message.get("blocks") is not None:
        yield channel_id, message


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
        yield channel_id, new_message


def add_thread_to_message(channel_id, message):
    thread_ts = message.get("thread_ts")
    message_ts = message["ts"]
    if thread_ts is not None and thread_ts == message_ts:
        thread = fetch_thread_for_message(channel_id, thread_ts)
        message["thread"] = list(
            process_message_in_thread(message) for message in thread
        )
    yield message


def get_raw_threads_graph(day):
    graph = bonobo.Graph()
    graph.add_chain(
        db.JsonRawMessagesDateReader(day),
        ChannelExtractor(),
        MessagesFetcher(day, day + datetime.timedelta(days=1)),
        remove_invalid_messages,
        process_channel_message,
        add_thread_to_message,
        db.JsonRawThreadsWriter(day)
    )
    return graph


def get_raw_threads_services(base_services):
    return base_services.copy()


def update_raw_threads(
        start_date,
        end_date,
        backdate_nworking_days,
        base_services
):
    database = base_services["database"]
    status_db = db.Status(database)
    services = get_raw_threads_services(base_services)
    ndays_ago = nworking_days_before(
        datetime.date.today(), backdate_nworking_days)
    for date in date_range(start_date, end_date):
        if not status_db.is_raw_threads_complete(date):
            logging.info("Fetching raw threads for %s", date.isoformat())
            graph = get_raw_threads_graph(date)
            bonobo.run(graph, services=services)
            if date < ndays_ago:
                status_db.set_raw_threads_complete(date)
        else:
            logging.info("Date %s is complete. Skipping.", date.isoformat())
