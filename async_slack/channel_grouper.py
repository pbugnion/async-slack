from bonobo.config import Configurable, ContextProcessor, use_raw_input
from bonobo.util import ValueHolder


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
