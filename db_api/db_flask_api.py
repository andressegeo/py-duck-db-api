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
            db_parser_def,
            db_host
    ):
        self._db_api_def = db_api_def
        self.db_connection = db_connection_def(
            db_api_def=db_api_def,
            user=db_user,
            password=db_password,
            database=db_name,
            host=db_host
        )

        self.__db_parser_def = db_parser_def

    def handle_description(self, request, table):
        code = 200

        columns = self.db_connection.get_columns(table)

        db_parser = self.__db_parser_def(
            table=table,
            columns=columns
        )

        result = {
            u"columns": db_parser.generate_column_description(
                table=table,
                columns=columns
            ),
            u"table": table
        }

        return jsonify(result), code

    def handle_request(self, request, table):
        result = {}

        code = 200
        db_parser = self.__db_parser_def(
            table=table,
            columns=self.db_connection.get_columns(table)
        )

        filters = request.args.get(u'filters')
        data = request.data

        if filters is not None:
            filters = json.loads(filters, encoding=u"utf-8")

        if data is not None and data != u"":
            data = json.loads(data, encoding=u"utf-8")

        dependencies = db_parser.generate_dependencies(filters=filters)
        if request.method == u"GET":

            items = self.db_connection.select(
                *dependencies,
                formater=db_parser.rows_to_formated
            )

            result = {
                u"items": items
            }

        elif request.method == u"PUT":

            count = self.db_connection.update(
                table=table,
                update=db_parser.parse_update(
                    data=data
                ),
                joins=dependencies[2],
                where=dependencies[3]
            )

            result = {
                u"count": count
            }

        elif request.method == u"DELETE":

            count = self.db_connection.delete(
                table=table,
                joins=dependencies[2],
                where=dependencies[3]
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
