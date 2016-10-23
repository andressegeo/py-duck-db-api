# -*- coding: utf-8 -*-

from db_api.db_connection import DBConnection
from flask import jsonify
import json

class DBFlaskAPI(object):

    def __init__(
            self,
            db_api_def,
            db_user,
            db_password,
            db_name,
            db_connection_def,
            db_parser_def
    ):

        self.db_connection = db_connection_def(
            db_api_def=db_api_def,
            user=db_user,
            password=db_password,
            database=db_name
        )

        self.__db_parser_def = db_parser_def

    def handle_request(self, request, table):
        result = {}

        db_parser = self.__db_parser_def(
            table=table,
            columns=self.db_connection.get_columns(table),
            referenced=self.db_connection.get_referenced(table)
        )

        filters = request.args.get(u'filters')
        data = request.data

        if filters is not None:
            filters = json.loads(filters, encoding=u"utf-8")

        if data is not None and data != u"":

            data = json.loads(data, encoding=u"utf-8")

        if request.method == u"GET":
            headers, rows = self.db_connection.select(
                table=table,
                where=db_parser.parse_filters(filters)
            )

            result = {
                u"items": db_parser.rows_to_json(table, headers, rows)
            }

        elif request.method == u"PUT":
            count = self.db_connection.update(
                table=table,
                update=db_parser.parse_update(
                    data=data
                ),
                where=db_parser.parse_filters(filters)
            )

            result = {
                u"count": count
            }

        return jsonify(result)