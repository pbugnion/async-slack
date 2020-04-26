import datetime
from pathlib import Path
from typing import NamedTuple, List

import toml

CONFIGURATION_LOCATION = Path.home() / ".config/async-slack/config.toml"


class Configuration(NamedTuple):
    database_directory: Path
    start_date: datetime.date
    end_date: datetime.date
    threads_lookback_working_days: int
    token_command: List[str]


def read_configuration() -> Configuration:
    raw_configuration = toml.load(CONFIGURATION_LOCATION)
    database_directory = Path(raw_configuration["database_directory"])
    start_date = raw_configuration["start_date"]
    end_date = raw_configuration.get(
        "end_date", datetime.date.today() + datetime.timedelta(days=1))
    threads_lookback_working_days = raw_configuration[
        "threads_lookback_working_days"
    ]
    token_command = raw_configuration["slack-token"]["subprocess"]["command"]
    configuration = Configuration(
        database_directory,
        start_date, end_date,
        threads_lookback_working_days,
        token_command
    )
    return configuration
