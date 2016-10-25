# -*- coding: utf-8 -*-


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
        self._db_api_def = db_api_def
        self.db_connection = db_connection_def(
            db_api_def=db_api_def,
            user=db_user,
            password=db_password,
            database=db_name
        )

        self.__db_parser_def = db_parser_def

    def handle_request(self, request, table):
        result = {}

        code = 200
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

        elif request.method == u"DELETE":

            count = self.db_connection.delete(
                table=table,
                where=db_parser.parse_filters(filters)
            )

            result = {
                u"count": count
            }

        elif request.method == u"POST":

            try:
                count = self.db_connection.insert(
                    insert=db_parser.parse_insert(data=data)
                )
                result = {
                    u"id": count
                }

                code = 201
            except self._db_api_def.OperationalError as e:
                result = {
                    u"message" : unicode(e[1])
                }

                code = 422

        return jsonify(result), code
