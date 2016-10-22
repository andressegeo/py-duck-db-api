# -*- coding: utf-8 -*-

from db_api.db_connection import DBConnection
from flask import jsonify


class DBFlaskAPI(object):

    def __init__(
            self,
            db_connection,
            db_parser
    ):

        self.db_connection = db_connection
        self.db_parser = db_parser

    def handle_request(self, request, table):
        result = {}

        if request.method == u"GET":

            filters = request.args.get(u'filters')

            filters = {
                u"$eq": [u"$issue", u"test"]
            }

            headers, rows = self.db_connection.select(
                table=table,
                where=self.db_parser.parse_filters(filters)
            )

            result = self.db_parser.rows_to_json(table, headers, rows)

            print(headers)
            print(rows)


        return jsonify(result)