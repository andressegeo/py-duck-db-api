# -*- coding: utf-8 -*-

from db_flask_api import DBFlaskAPI
from db_connection import DBConnection
from db_parser import DBParser
from db_export import DBExport
from flask import Blueprint, request


def construct_db_api_blueprint(
    db_driver,
    db_host,
    db_user,
    db_passwd,
    db_name
):
    """
    Construct a blue print allowing to handle the database access (filtering, insertion,
    update, delete, etc).
    :param db_driver:
    :param db_host:
    :param db_user:
    :param db_passwd:
    :param db_name:
    :return: A blue print with the DB Api configured
    """

    db_flask_api = DBFlaskAPI(
        db_api_def=db_driver,
        db_user=db_user,
        db_password=db_passwd,
        db_name=db_name,
        db_connection_def=DBConnection,
        db_parser_def=DBParser,
        db_host=db_host,
        db_export_def=DBExport
    )

    db_api_blueprint = Blueprint(u'db_api', __name__)

    # Example : http://localhost:5000/api/db/hour/export?pipeline=[{"$project":{"user_email":"$affected_to.email","id":1}}]&options={"type":"csv","delimiter":";"}
    @db_api_blueprint.route(u'/<string:table>/export', methods=[u"GET"])
    def table_aggregation_export(table):
        return db_flask_api.handle_export(request, table=table)

    @db_api_blueprint.route(u'/<string:table>/aggregation', methods=[u"GET", u"POST"])
    def table_aggregation(table):
        return db_flask_api.handle_aggregation(request, table=table)

    @db_api_blueprint.route(u'/<string:table>', methods=[u"POST", u"PUT", u"DELETE", u"GET"])
    def table_request(table):
        return db_flask_api.handle_request(request, table=table)

    @db_api_blueprint.route(u'/<string:table>/description', methods=[u"GET"])
    def table_description_request(table):
        return db_flask_api.handle_description(request, table=table)

    return db_api_blueprint
