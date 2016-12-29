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
            u"fields": db_parser.generate_column_description(
                table=table,
                columns=columns
            ),
            u"table": table
        }

        return jsonify(result), code

    def handle_aggregation(self, request, table):
        result = {}

        code = 200
        db_parser = self.__db_parser_def(
            table=table,
            columns=self.db_connection.get_columns(table)
        )

        pipeline = json.loads(request.args.get(u'pipeline', []))
        base_dependencies = db_parser.generate_base_state()
        stages = []
        custom_dependencies = None
        for stage in pipeline:
            if u"$match" in stage:
                stages.append(
                    (
                        u"$match", db_parser.parse_match(
                            stage.get(u"$match", {}),
                            use_alias=True,
                            is_formated=custom_dependencies is None,
                            from_state=custom_dependencies
                        )
                    )
                )
            elif u"$project" in stage:
                ret = db_parser.parse_project(
                    stage.get(u"$project"),
                    dependencies=custom_dependencies
                )
                stages.append(
                    (u"$project", ret)
                )
                custom_dependencies = ret[u'dependencies']

        def formater(headers, rows, fields):
            return db_parser.rows_to_formated(headers, rows, fields, is_formated=custom_dependencies is None)

        items = self.db_connection.aggregate(
            base_dependencies, 
            formater=formater,
            stages=stages
        )

        result[u'items'] = items

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

        base_state = db_parser.generate_base_state()
        match_state = db_parser.parse_match(match=filters, from_state=base_state)

        if request.method == u"GET":

            items = self.db_connection.select(
                fields=base_state.get(u"fields"),
                table=table,
                joins=base_state.get(u"joins"),
                where=match_state,
                formater=db_parser.rows_to_formated,
                first=request.args.get(u"first"),
                nb=request.args.get(u"nb")
            )

            result = {
                u"items": items,
                u"first": int(request.args.get(u'first', 0)),
                u"nb": int(request.args.get(u'nb', 100))
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
