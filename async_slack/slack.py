import os
from functools import lru_cache, wraps
import logging

from slacker import Slacker
from tenacity import stop_after_attempt, wait_exponential, retry, after_log


@lru_cache(maxsize=1)
def get_client():
    SLACK_TOKEN = os.environ["SLACK_TOKEN"]
    return Slacker(SLACK_TOKEN)


api_retry = retry(
    wait=wait_exponential(multiplier=1, min=60, max=1800),
    reraise=True,
    stop=stop_after_attempt(10),
    after=after_log(logging.getLogger("slack-api"), log_level=logging.INFO)
)
