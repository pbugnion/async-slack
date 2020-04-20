import argparse
import logging
import datetime
from contextlib import contextmanager
import time

import bonobo

from .users_graph import get_users_graph, get_users_services
from .channels_graph import get_channels_graph, get_channels_services
from .message_count_graph import update_message_count, update_message_count_quick
from .raw_threads_graph import update_raw_threads, update_raw_threads_quick
from .enriched_messages_graph import (
    get_enriched_messages_graph, get_enriched_messages_services
)
from .convert_to_org_graph import (
    get_convert_to_org_graph,
    get_convert_to_org_services
)

from . import db
from .config import read_configuration
from .date_utils import nworking_days_before


logging.basicConfig(level=logging.INFO)


def get_services(configuration):
    return {
        "database": db.JsonFsDatabase(configuration.database_directory)
    }


def make_parser():
    parser = argparse.ArgumentParser("async-update-slack")
    parser.add_argument("--quick", action="store_true")
    return parser


@contextmanager
def log_timed(name):
    t_start = time.time()
    yield
    duration = time.time() - t_start
    duration_readable = str(int(duration * 1000) / 1000.0) + "s"
    logging.info("Time to run %s: %s", name, duration_readable)


def main():
    configuration = read_configuration()
    parser = make_parser()
    arguments = parser.parse_args()
    logging.info(
        "Running with configuration %s and arguments %s.",
        configuration, arguments
    )
    base_services = get_services(configuration)
    logging.info("Getting users")
    with log_timed("users graph"):
        bonobo.run(
            get_users_graph(),
            services=get_users_services(base_services)
        )
    logging.info("Getting channels")
    with log_timed("channels graph"):
        bonobo.run(
            get_channels_graph(),
            services=get_channels_services(base_services)
        )
    logging.info("Getting message count")
    with log_timed("message count graph"):
        if arguments.quick:
            update_message_count_quick(datetime.date.today(), base_services)
        else:
            update_message_count(
                configuration.start_date,
                configuration.end_date,
                base_services
            )
    logging.info("Getting raw threads.")
    with log_timed("raw threads graph"):
        if arguments.quick:
            update_raw_threads_quick(
                nworking_days_before(datetime.date.today(), 1),
                configuration.end_date,
                base_services
            )
        else:
            update_raw_threads(
                configuration.start_date,
                configuration.end_date,
                configuration.threads_lookback_working_days,
                base_services
            )
    logging.info("Enriching messages with user and channel information")
    bonobo.run(
        get_enriched_messages_graph(
            configuration.start_date,
            configuration.end_date
        ),
        services=get_enriched_messages_services(base_services)
    )
    logging.info("Converting to org-mode")
    bonobo.run(
        get_convert_to_org_graph(),
        services=get_convert_to_org_services(base_services)
    )
