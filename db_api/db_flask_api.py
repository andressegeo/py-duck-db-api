# -*- coding: utf-8 -*-


from flask import jsonify, make_response
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
            db_host,
            db_export_def
    ):
        self.__db_parser_def = db_parser_def
        self.__db_api_def = db_api_def

        self.db_connection = db_connection_def(
            db_api_def=db_api_def,
            user=db_user,
            password=db_password,
            database=db_name,
            host=db_host
        )

        self.db_export = db_export_def()

    def handle_export(self, request, table):
        code = 200

        params = {
            u"pipeline": None,
            u"filters": None,
            u"options": {}
        }

        for key in params:
            var = request.args.get(key, None)
            if var is not None:
                params[key] = json.loads(var, encoding=u"utf-8")

        db_parser = self.__db_parser_def(
            table=table,
            columns=self.db_connection.get_columns(table)
        )
        base_state = db_parser.generate_base_state()
        if params[u"pipeline"] is not None:
            stages = self._pipeline_to_stages(
                db_parser=db_parser,
                pipeline=params[u"pipeline"]
            )

            headers, rows = self.db_connection.aggregate(
                table,
                base_state=base_state,
                stages=stages
            )

        elif params[u"filters"] is not None:
            filters = db_parser.parse_match(match=params[u"filters"], from_state=base_state)
            headers, rows = self.db_connection.select(
                fields=base_state.get(u"fields"),
                table=table,
                joins=base_state.get(u"joins"),
                where=filters,
                first=request.args.get(u"first"),
                nb=request.args.get(u"nb")
            )
        else:
            return jsonify({
                u"message": u"Unrecognized export"
            }), 422

        export = self.db_export.export(headers, rows, params[u"options"])
        output = make_response(export.getvalue())
        output.headers[U"Content-Disposition"] = U"attachment; filename=export.csv"
        output.headers[U"Content-type"] = U"text/csv"
        return output, code

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

    def _pipeline_to_stages(self, db_parser, pipeline):
        base_state = db_parser.generate_base_state()
        stages = []
        custom_state = None

        for stage in pipeline:
            if u"$match" in stage:
                ret = db_parser.parse_match(
                    match=stage.get(u"$match", {}),
                    from_state=custom_state or base_state
                )
                stages.append(
                    {
                        u"type": u"match",
                        u"parsed": ret
                    }
                )
            elif u"$project" in stage:
                ret = db_parser.parse_project(
                    stage.get(u"$project"),
                    from_state=custom_state or base_state
                )
                stages.append(
                    {
                        u"type": u"project",
                        u"parsed": ret
                    }
                )
                # Project alter the state. Use custom one
                custom_state = ret[u'state']
            elif u"$group" in stage:
                ret = db_parser.parse_group(
                    group=stage.get(u"$group"),
                    from_state=custom_state or base_state
                )
                stages.append(
                    {
                        u"type": u"group",
                        u"parsed": ret
                    }
                )
                # Group alter the state. Use custom one
                custom_state = ret[u'state']
            elif u"$orderby" in stage:
                ret = db_parser.parse_order_by(
                    order_by=stage.get(u"$orderby"),
                    from_state=custom_state or base_state
                )
                stages.append(
                    {
                        u"type": u"orderby",
                        u"parsed": ret
                    }
                )

        return stages

    def handle_aggregation(self, request, table):
        result = {}

        code = 200
        db_parser = self.__db_parser_def(
            table=table,
            columns=self.db_connection.get_columns(table)
        )

        # Get pipeline from payload or url arg
        data = request.data

        if data is not None and data != u"":
            data = json.loads(data, encoding=u"utf-8")
        else:
            data = { u"pipeline": [] }
        pipeline = json.loads(request.args.get(u'pipeline', u"[]")) or data.get(u"pipeline", [])

        stages = self._pipeline_to_stages(
            db_parser=db_parser,
            pipeline=pipeline
        )

        items = self.db_connection.aggregate(
            table,
            db_parser.generate_base_state(),
            stages,
            formater=db_parser.rows_to_formated
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
        order_by = request.args.get(u"order_by")
        export_to = request.args.get(u'export_to')
        data = request.data

        if filters is not None:
            filters = json.loads(filters, encoding=u"utf-8")

        if order_by is not None:
            order_by = json.loads(order_by, encoding=u"utf-8")

        if data is not None and data != u"":
            data = json.loads(data, encoding=u"utf-8")

        base_state = db_parser.generate_base_state()


        try:
            if request.method == u"GET":
                filters = db_parser.parse_match(
                    match=filters,
                    from_state=base_state
                )

                order_by = db_parser.parse_order_by(
                    order_by=order_by,
                    from_state=base_state
                )
                items = self.db_connection.select(
                    fields=base_state.get(u"fields"),
                    table=table,
                    joins=base_state.get(u"joins"),
                    where=filters,
                    formatter=db_parser.rows_to_formated,
                    order_by=order_by,
                    first=request.args.get(u"first"),
                    nb=request.args.get(u"nb")
                )

                result = {
                    u"items": items,
                    u"first": int(request.args.get(u'first', 0)),
                    u"nb": int(request.args.get(u'nb', 100))
                }

            elif request.method == u"PUT":
                filters = db_parser.parse_match(
                    match=filters,
                    from_state=base_state,
                    filter_with_alias=False
                )
                count = self.db_connection.update(
                    table=table,
                    update=db_parser.parse_update(
                        data=data
                    ),
                    joins=base_state.get(u"joins"),
                    where=filters
                )

                result = {
                    u"count": count
                }

            elif request.method == u"DELETE":
                filters = db_parser.parse_match(
                    match=filters,
                    from_state=base_state,
                    filter_with_alias=False
                )

                if filters.get(u"statements") == u"":
                    raise ValueError(u"You need to set a proper filter to delete (safe mode)")

                count = self.db_connection.delete(
                    table=table,
                    joins=base_state.get(u"joins"),
                    where=filters,
                )

                result = {
                    u"count": count
                }

            elif request.method == u"POST":

                insert = db_parser.parse_insert(data=data)

                count = self.db_connection.insert(
                    table=db_parser._table,
                    fields=insert[u"fields"],
                    positional_values=insert[u'positional_values'],
                    values=insert[u"values"]
                )

                result = {
                    u"id": count
                }

                code = 201
        except ValueError as e:
            result = {
                u"message": unicode(e)
            }
            code = 422

        except self.__db_api_def.OperationalError as e:
            result = {
                u"message": unicode(e[1])
            }
            code = 422

        return jsonify(result), code
