import textwrap
from datetime import datetime

import bonobo
from bonobo.config import Configurable, ContextProcessor, use_raw_input, use
from bonobo.util import ValueHolder

from emoji import emojize

from . import db
from .orger.inorganic import node, link, timestamp


class ChannelGrouper(Configurable):

    @ContextProcessor
    def acc(self, context):
        channels = yield ValueHolder({})
        for channel, messages in channels.items():
            context.send(channel, messages)

    @use_raw_input
    def __call__(self, channels, message):
        channel_id = message.get("channel")
        if channel_id:
            current_channels = channels.get()
            channel = current_channels.setdefault(channel_id, [])
            channel.append(message)
            channels.set(current_channels)


class BlockRenderer:

    def __init__(self, users, channels):
        self._users = users
        self._channels = channels
    
    def render(self, block):
        try:
            elements = block['elements']
        except KeyError:
            elements = []
        return "\n".join(self._element(element) for element in elements)

    def _rich_text_section(self, element):
        return "".join(self._element(element) for element in element['elements'])

    def _text(self, element):
        return element['text']
    
    
    def _rich_text_list(self, element):
        rendered_elements = [self._element(element) for element in element['elements']]
        list_elements = [f"- {element}" for element in rendered_elements]
        return "\n".join(list_elements)
    
    def _link(self, element):
        try:
            text = element['text']
        except KeyError:
            text = element['url']
        return link(url=element['url'], title=text)
    
    
    def _broadcast(self, element):
        return f"@{element['range']}"
    
    
    def _user(self, element):
        user_id = element['user_id']
        try:
            user_name = self._users.for_id(user_id)['name']
        except KeyError:
            user_name = user_id
        return f"@{user_name}"
    
    
    def _channel(self, element):
        channel_id = element['channel_id']
        try:
            channel_name = self._channels.for_id(channel_id)["derived_name"]
        except KeyError:
            channel_name = channel_id
        return f"#{channel_name}"


    def _debug(self, element):
        return str(element)
    
    
    def _emoji(self, element):
        name = element['name']
        return emojize(f":{name}:")


    def _empty(self, element):
        return ""
            
    
    def _element(self, element):
        element_type = element['type']
        return {
            'text': self._text,
            'rich_text_section':self. _rich_text_section,
            'rich_text_list': self._rich_text_list,
            'link': self._link,
            'broadcast': self._broadcast,
            'user': self._user,
            'usergroup': self._debug,
            'channel': self._channel,
            'emoji': self._emoji,
            'rich_text_preformatted': self._rich_text_section,
            'rich_text_quote': self._rich_text_section,
            'button': self._empty,
            'mrkdwn': self._empty
        }[element_type](element)


def transform_message(message, block_renderer):
    blocks = message.get('blocks')
    if blocks:
        body = "\n".join(block_renderer.render(block) for block in blocks)
        message_time = datetime.fromtimestamp(float(message['ts']))
        user = message.get('user_name') or message.get('user') or 'UNKNOWN'
        return {
            "body": body,
            "user": user,
            "timestamp": timestamp(message_time, inactive=True)
        }


def transform_thread(thread, block_renderer):
    transformed_thread = []
    if thread is not None:
        for reply in thread:
            transformed_reply = transform_message(reply, block_renderer)
            if transformed_reply is not None:
                transformed_thread.append(transformed_reply)
    return transformed_thread


@use("block_renderer")
def yield_message(message, block_renderer):
    transformed_message = transform_message(message, block_renderer)
    if transformed_message is not None:
        transformed_message["channel"] = message["channel"]
        thread = message.get("thread")
        if thread is not None:
            transformed_message["thread"] = transform_thread(
                thread, block_renderer)
        yield transformed_message


@use("block_renderer", "channels")
def convert_channel_to_node(channel_id, messages, block_renderer, channels):
    nodes_for_channel = []
    for message in messages:
        nodes_for_channel.extend(list(yield_message(block_renderer, message)))
    if nodes_for_channel:
        channel_name = channels.for_id(channel_id)["derived_name"]
        heading = f"#{channel_name}"
        body = "\n".join(nodes_for_channel)
        # Yield as a single element list to avoid
        # bonobo untuplizing the node
        yield [node(heading=heading, body=body)]


@use("channels")
def yield_channel(channel_id, messages, channels):
    channel = channels.for_id(channel_id)
    yield {
        "channel": channel,
        "messages": sorted(messages, key=lambda message: message["timestamp"], reverse=True)
    }


def render_node(enveloped_node):
    [node] = enveloped_node
    yield node.render() + "\n\n"


def get_convert_to_org_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        db.JsonEnrichedMessagesReader(),
        yield_message,
        ChannelGrouper(),
        yield_channel,
        db.JsonOrgMessagesWriter()
    )
    return graph


def get_convert_to_org_services(base_services, **options):
    database = base_services["database"]
    users = db.Users(database)
    channels = db.Channels(database)
    block_renderer = BlockRenderer(users, channels)
    return {
        **base_services,
        "channels": channels,
        "block_renderer": block_renderer
    }
    
