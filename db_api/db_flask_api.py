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
            headers=self.db_connection.get_headers(table),
            referenced=self.db_connection.get_referenced(table)
        )

        filters = request.args.get(u'filters')
        if filter is not None:
            filters = json.loads(filters, encoding=u"utf-8")

        if request.method == u"GET":
            headers, rows = self.db_connection.select(
                table=table,
                where=db_parser.parse_filters(filters)
            )

            result = db_parser.rows_to_json(table, headers, rows)



        return jsonify(result)