import datetime
from pathlib import Path
from typing import NamedTuple

import toml

CONFIGURATION_LOCATION = Path.home() / ".config" / "async-slack" / "config.toml"


class Configuration(NamedTuple):
    database_directory: Path
    start_date: datetime.date
    end_date: datetime.date

def read_configuration():
    raw_configuration = toml.load(CONFIGURATION_LOCATION)
    database_directory = Path(raw_configuration["database_directory"])
    start_date = raw_configuration["start_date"]
    end_date = raw_configuration.get("end_date", datetime.date.today() + datetime.timedelta(days=1))
    return Configuration(database_directory, start_date, end_date)

