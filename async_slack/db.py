import json
import logging
from contextlib import contextmanager
from pathlib import Path
from functools import lru_cache

import bonobo
from bonobo.config import ContextProcessor, use_context, Configurable, Service
from bonobo.constants import NOT_MODIFIED


class JsonFsDatabase:

    def __init__(self, root):
        self._root = Path(root)
        self._fs = bonobo.open_fs(self._root)
        self._users_file_name = "users.json"
        self._channels_file_name = "channels.json"
        self._raw_messages_file_name = "raw-messages.json"
        self._enriched_messages_file_name = "enriched-messages.json"

    @contextmanager
    def open_users_file(self, mode="r"):
        with self._fs.open(self._users_file_name, mode, encoding="utf-8") as fp:
            yield fp

    @contextmanager
    def open_channels_file(self, mode="r"):
        with self._fs.open(self._channels_file_name, mode, encoding="utf-8") as fp:
            yield fp

    @contextmanager
    def open_raw_messages_file(self, mode="r"):
        with self._fs.open(self._raw_messages_file_name, mode, encoding="utf-8") as fp:
            yield fp

    @contextmanager
    def open_enriched_messages_file(self, mode="r"):
        with self._fs.open(self._enriched_messages_file_name, mode, encoding="utf-8") as fp:
            yield fp


class Users:

    def __init__(self, database):
        self._database = database
        self._data = self._read_input()

    def _read_input(self):
        with self._database.open_users_file() as fp:
            lines = [json.loads(line) for line in fp]
        return lines

    @lru_cache()
    def for_id(self, user_id):
        for user in self._data:
            if user["id"] == user_id:
                return user
        else:
            raise KeyError(user_id)


class Channels:

    def __init__(self, database):
        self._database = database
        self._data = self._read_input()

    def _read_input(self):
        with self._database.open_channels_file() as fp:
            lines = [json.loads(line) for line in fp]
        return lines

    def all(self):
        yield from self._data.copy()

    @lru_cache()
    def for_id(self, channel_id):
        for channel in self._data:
            if channel["id"] == channel_id:
                return channel
        else:
            raise KeyError(channel_id)


@use_context
class _FileLdJsonWriter(Configurable):

    database = Service("database")

    def open(self, database):
        pass

    @ContextProcessor
    def fp(self, _, *, database):
        with self.open(database) as fp:
            yield fp
            fp.write("\n")

    def __call__(self, fp, context, entry, *, database):
        context.setdefault("lineno", 0)
        line = ("\n" if context.lineno else "") + json.dumps(entry)
        fp.write(line)
        fp.flush()
        context.lineno += 1
        return NOT_MODIFIED


@use_context
class JsonUserWriter(_FileLdJsonWriter):

    def open(self, database):
        return database.open_users_file("w")


@use_context
class JsonChannelsWriter(_FileLdJsonWriter):

    def open(self, database):
        return database.open_channels_file("w")


@use_context
class JsonRawMessagesWriter(_FileLdJsonWriter):

    def open(self, database):
        return database.open_raw_messages_file("w")


@use_context
class JsonEnrichedMessagesWriter(_FileLdJsonWriter):

    def open(self, database):
        return database.open_enriched_messages_file("w")


@use_context
class JsonRawMessagesReader(Configurable):

    database = Service("database")

    @ContextProcessor
    def fp(self, _, *, database):
        with database.open_raw_messages_file() as fp:
            yield fp

    def __call__(self, fp, _, *, database):
        for line in fp:
            yield json.loads(line)


@use_context
class JsonEnrichedMessagesReader(Configurable):

    database = Service("database")

    @ContextProcessor
    def fp(self, _, *, database):
        with database.open_enriched_messages_file() as fp:
            yield fp

    def __call__(self, fp, _, *, database):
        for line in fp:
            yield json.loads(line)