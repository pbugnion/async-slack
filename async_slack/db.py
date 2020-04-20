import json
from contextlib import contextmanager
from pathlib import Path
from functools import lru_cache

import bonobo
from bonobo.config import (
    ContextProcessor, use_context, Configurable, Service, Option)
from bonobo.constants import NOT_MODIFIED

import fs.errors


class JsonFsDatabase:

    def __init__(self, root):
        self._root = Path(root)
        self._fs = bonobo.open_fs(self._root, create=True)
        self._users_file_name = "users.json"
        self._channels_file_name = "channels.json"
        self._enriched_messages_file_name = "enriched-messages.json"
        self._message_count_file = "message-count.json"
        self._status_file_name = "status.json"
        self._org_messages_file_name = "org-messages.json"

    def _get_raw_threads_file_name(self, date):
        return f"raw-threads-{date.isoformat()}.json"

    @contextmanager
    def _open(self, file_name, mode):
        with self._fs.open(file_name, mode, encoding="utf-8") as fp:
            yield fp

    @contextmanager
    def open_status_file(self, mode="r"):
        with self._open(self._status_file_name, mode) as fp:
            yield fp

    @contextmanager
    def open_users_file(self, mode="r"):
        with self._open(self._users_file_name, mode) as fp:
            yield fp

    @contextmanager
    def open_channels_file(self, mode="r"):
        with self._open(self._channels_file_name, mode) as fp:
            yield fp

    @contextmanager
    def open_raw_threads_file(self, date, mode="r"):
        file_name = self._get_raw_threads_file_name(date)
        with self._open(file_name, mode) as fp:
            yield fp

    @contextmanager
    def open_enriched_messages_file(self, mode="r"):
        with self._open(self._enriched_messages_file_name, mode) as fp:
            yield fp

    @contextmanager
    def open_org_messages_file(self, mode="r"):
        with self._open(self._org_messages_file_name, mode) as fp:
            yield fp

    @contextmanager
    def open_message_count_file(self, mode="r"):
        with self._open(self._message_count_file, mode) as fp:
            yield fp


class Status:

    def __init__(self, database):
        self._database = database
        self._data = self._read_input()

    def _read_input(self):
        try:
            with self._database.open_status_file() as fp:
                entries = json.load(fp)
        except fs.errors.ResourceNotFound:
            entries = {}
        return entries

    def _write_input(self):
        with self._database.open_status_file("w") as fp:
            json.dump(self._data, fp)

    def is_message_count_complete(self, date):
        try:
            complete = self._data[date.isoformat()]["message_count_complete"]
        except KeyError:
            complete = False
        return complete

    def is_raw_threads_complete(self, date):
        try:
            complete = self._data[date.isoformat()]["raw_threads_complete"]
        except KeyError:
            complete = False
        return complete

    def set_message_count_complete(self, date):
        status_for_date = self._data.setdefault(date.isoformat(), {})
        status_for_date["message_count_complete"] = True
        self._write_input()

    def set_raw_threads_complete(self, date):
        status_for_date = self._data.setdefault(date.isoformat(), {})
        status_for_date["raw_threads_complete"] = True
        self._write_input()


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


class MessageCount:

    def __init__(self, database):
        self._database = database
        self._data = self._read_input()

    def _read_input(self):
        try:
            with self._database.open_message_count_file() as fp:
                entries = json.load(fp)
        except fs.errors.ResourceNotFound:
            entries = {}
        return entries

    def _write_input(self):
        with self._database.open_message_count_file("w") as fp:
            json.dump(self._data, fp)

    def set_day_channel(self, date, channel, count):
        self._data.setdefault(date.isoformat(), {})[channel] = count
        self._write_input()

    def get_channels_for_day(self, date):
        return self._data.get(date.isoformat(), {}).keys()


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
class JsonRawThreadsWriter(_FileLdJsonWriter):

    date = Option(required=True, positional=True)
    database = Service("database")

    def open(self, database):
        return database.open_raw_threads_file(self.date, mode="w")


@use_context
class JsonEnrichedMessagesWriter(_FileLdJsonWriter):

    def open(self, database):
        return database.open_enriched_messages_file("w")


# @use_context
# class JsonRawMessagesReader(Configurable):

#     database = Service("database")

#     def __call__(self, _, date, *, database):
#         with database.open_raw_messages_file(date) as fp:
#             for line in fp:
#                 if line.strip():
#                     yield json.loads(line)


@use_context
class JsonRawThreadsReader(Configurable):

    database = Service("database")

    def __call__(self, _, date, *, database):
        with database.open_raw_threads_file(date) as fp:
            for line in fp:
                if line.strip():
                    yield json.loads(line)


@use_context
class JsonRawMessagesDateReader(Configurable):

    date = Option(required=True, positional=True)
    database = Service("database")


    @ContextProcessor
    def fp(self, _, *, database):
        with database.open_raw_messages_file(self.date) as fp:
            yield fp

    def __call__(self, fp, _, *, database):
        for line in fp:
            if line.strip():
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


@use_context
class JsonOrgMessagesWriter(_FileLdJsonWriter):

    def open(self, database):
        return database.open_org_messages_file("w")
