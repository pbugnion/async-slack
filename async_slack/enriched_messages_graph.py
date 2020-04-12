
import bonobo
from bonobo.config import use

from . import db


@use("users")
def add_user(message, users):
    message = message.copy()
    try:
        user_id = message["user"]
        user = users.for_id(user_id)
    except KeyError:
        pass
    else:
        message["user_name"] = user["name"]
        message["user_full_name"] = user["real_name"]
    yield message


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


def get_enriched_messages_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        db.JsonRawMessagesReader(),
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
    
