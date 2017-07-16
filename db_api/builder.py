# -*- coding: utf-8 -*-

from .db_api import DBApi
from .db_connection import DBConnection
from .db_parser import DBParser


def build_db_api(
    db_api_def,
    db_user,
    db_password,
    db_name,
    db_host=u"127.0.0.1"
):
    """
    Build the DB Api.
    Args:
        db_api_def (DB connection class)
        db_user (unicode): The user name.
        db_password (unicode): The user password.
        db_name (unicode): The DB name.
        host (unicode): The DB IP.

    Returns:
        (DBApi)
    """

    return DBApi(
        db_connection=DBConnection(
            db_api_def,
            user=db_user,
            password=db_password,
            database=db_name,
            host=db_host
        ),
        db_parser_def=DBParser
    )
