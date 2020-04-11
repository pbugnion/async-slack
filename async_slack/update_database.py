import logging
from pathlib import Path

import bonobo

from .users_graph import get_users_graph, get_users_services
from .channels_graph import get_channels_graph, get_channels_services
from .raw_messages_graph import get_raw_messages_graph, get_raw_messages_services

from . import db


logging.basicConfig(level=logging.INFO)


def get_services(**options):
    return {
        "database": db.JsonFsDatabase(Path("/project"))
    }


def main():
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        base_services = get_services(**options)
        logging.info("Getting users")
        bonobo.run(
            get_users_graph(**options),
            services=get_users_services(base_services, **options)
        )
        logging.info("Getting channels")
        bonobo.run(
            get_channels_graph(**options),
            services=get_channels_services(base_services, **options)
        )
        logging.info("Getting raw messages")
        bonobo.run(
            get_raw_messages_graph(**options),
            services=get_raw_messages_services(base_services, **options)
        )
