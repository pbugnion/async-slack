import bonobo
from bonobo.config import use

from . import slack
from . import db
from .dict_utils import map_dictionary


@slack.api_retry
def get_conversations(client, types, cursor=None):
    return client.conversations.list(types=types, cursor=cursor)


@use("slack")
def get_channels(slack):
    types = "public_channel,private_channel,mpim,im"
    response = get_conversations(slack.client, types).body

    for channel in response["channels"]:
        yield channel

    while response["response_metadata"].get("next_cursor"):
        cursor = response["response_metadata"]["next_cursor"]
        response = get_conversations(slack.client, types, cursor=cursor).body
        for channel in response["channels"]:
            yield channel


def process_channel(channel):
    to_keep = {
        "name": "/name",
        "id": "/id",
        "is_member": "/is_member",
        "is_im": "/is_im",
        "is_mpim": "/is_mpim",
        "is_private": "/is_private",
        "user": "/user"
    }
    yield map_dictionary(to_keep, channel)


@use("users")
def get_derived_name(channel, users):
    if channel["is_im"]:
        user_id = channel["user"]
        try:
            user_name = users.for_id(user_id)["name"]
        except KeyError:
            user_name = None
        channel["derived_name"] = f"im:{user_name}"
    else:
        channel["derived_name"] = channel["name"]
    yield channel


def get_channels_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        get_channels,
        process_channel, 
        get_derived_name,
        db.JsonChannelsWriter()
    )
    return graph


def get_channels_services(base_services, **options):
    database = base_services["database"]
    return {**base_services, "users": db.Users(database)}
