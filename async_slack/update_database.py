import logging
from pathlib import Path
import datetime

import bonobo

from .users_graph import get_users_graph, get_users_services
from .channels_graph import get_channels_graph, get_channels_services
from .raw_messages_graph import update_raw_messages
from .raw_threads_graph import update_raw_threads
from .enriched_messages_graph import get_enriched_messages_graph, get_enriched_messages_services
from .convert_to_org_graph import get_convert_to_org_graph, get_convert_to_org_services

from . import db
from .config import read_configuration


logging.basicConfig(level=logging.INFO)


def get_services(configuration):
    return {
        "database": db.JsonFsDatabase(configuration.database_directory)
    }


def main():
    configuration = read_configuration()
    logging.info(f"Running with configuration {configuration}")
    base_services = get_services(configuration)
    logging.info("Getting users")
    bonobo.run(
        get_users_graph(),
        services=get_users_services(base_services)
    )
    logging.info("Getting channels")
    bonobo.run(
        get_channels_graph(),
        services=get_channels_services(base_services)
    )
    logging.info("Getting raw messages")
    update_raw_messages(
        configuration.start_date,
        configuration.end_date,
        base_services
    )
    logging.info("Getting raw threads.")
    update_raw_threads(
        configuration.start_date,
        configuration.end_date,
        configuration.threads_lookback_working_days,
        base_services
    )
    logging.info("Enriching messages with user and channel information")
    bonobo.run(
        get_enriched_messages_graph(configuration.start_date, configuration.end_date),
        services=get_enriched_messages_services(base_services)
    )
    logging.info("Converting to org-mode")
    bonobo.run(
        get_convert_to_org_graph(),
        services=get_convert_to_org_services(base_services)
    )
