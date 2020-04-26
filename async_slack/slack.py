import os
from functools import lru_cache, wraps
import logging
import subprocess

from slacker import Slacker
from tenacity import stop_after_attempt, wait_exponential, retry, after_log


class SlackClient:

    def __init__(self, token):
        self._client = Slacker(token)


    @classmethod
    def from_environment(cls):
        token = os.environ["SLACK_TOKEN"]
        return cls(token)


    @classmethod
    def from_command(cls, cmd):
        process = subprocess.run(cmd, capture_output=True, check=True, text=True)
        token = process.stdout.strip()
        return cls(token)
        

    @property
    def client(self):
        return self._client


api_retry = retry(
    wait=wait_exponential(multiplier=1, min=60, max=1800),
    reraise=True,
    stop=stop_after_attempt(10),
    after=after_log(logging.getLogger("slack-api"), log_level=logging.INFO)
)
