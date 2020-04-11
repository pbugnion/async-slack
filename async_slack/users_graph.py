import bonobo

from . import slack
from . import db
from .dict_utils import map_dictionary


def get_users():
    client = slack.get_client()
    response = client.users.list().body
    for member in response["members"]:
        yield member


def process_user(user):
    to_keep = {
        "name": "/name", 
        "id": "/id", 
        "real_name": "/profile/real_name",
    }
    yield map_dictionary(to_keep, user)


def get_users_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        get_users, 
        process_user, 
        db.JsonUserWriter()
    )
    return graph


def get_users_services(base_services, **options):
    return base_services
    
