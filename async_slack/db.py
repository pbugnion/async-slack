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

    @contextmanager
    def open_users_file(self, mode="r"):
        with self._fs.open(self._users_file_name, mode, encoding="utf-8") as fp:
            yield fp

    @contextmanager
    def open_channels_file(self, mode="r"):
        with self._fs.open(self._channels_file_name, mode, encoding="utf-8") as fp:
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


@use_context
class JsonUserWriter(Configurable):

    database = Service("database")

    @ContextProcessor
    def fp(self, _, *, database):
        with database.open_users_file("w") as fp:
            yield fp
            fp.write("\n")


    def __call__(self, fp, context, user, *, database):
        context.setdefault("lineno", 0)
        line = ("\n" if context.lineno else "") + json.dumps(user)
        fp.write(line)
        fp.flush()
        context.lineno += 1
        return NOT_MODIFIED


@use_context
class JsonChannelsWriter(Configurable):

    database = Service("database")

    @ContextProcessor
    def fp(self, _, *, database):
        with database.open_channels_file("w") as fp:
            yield fp
            fp.write("\n")


    def __call__(self, fp, context, channel, *, database):
        context.setdefault("lineno", 0)
        line = ("\n" if context.lineno else "") + json.dumps(channel)
        fp.write(line)
        fp.flush()
        context.lineno += 1
        return NOT_MODIFIED
