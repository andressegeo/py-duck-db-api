# -*- coding: utf-8 -*-


from flask import jsonify, make_response, Blueprint, request
from collections import OrderedDict
import json


class DBFlaskAPI(object):

    def __init__(
            self,
            db_api
    ):
        self._db_api = db_api

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

    def construct_blueprint(self):

        db_api_blueprint = Blueprint(u'db_api', __name__)

        @db_api_blueprint.route(u'/<string:table>', methods=[u"POST", u"PUT", u"DELETE", u"GET"])
        def table_request(table):
            return self.handle_request(request, table=table)

        @db_api_blueprint.route(u'/<string:table>/description', methods=[u"GET"])
        def table_description_request(table):
            return self.handle_description(request, table=table)

        return db_api_blueprint

    def handle_description(self, request, table):
        result = self._db_api.description(table)
        return jsonify(result), 200
    
    def handle_request(self, request, table):
        result = {}
        code = 200
        filters = request.args.get(u'filters')
        order_by = request.args.get(u"order_by")
        data = request.data

        if filters is not None:
            filters = json.loads(filters, encoding=u"utf-8")
        else:
            filters = {}

        if order_by is not None:
            order_by = json.loads(
                order_by,
                encoding=u"utf-8",
                object_pairs_hook=OrderedDict
            )
        else:
            order_by = {}

        if data is not None and data != u"":
            data = json.loads(data, encoding=u"utf-8")

        if request.method == u"GET":
            first = int(request.args.get(u'first', 0))
            nb = int(request.args.get(u'nb', 100))

            items, has_next = self._db_api.list(
                table=table,
                filters=filters,
                limit=nb,
                offset=first,
                order_by=[key for key in order_by],
                order=[order_by[key] for key in order_by]
            )

            result = {
                u"items": items,
                u"first": first,
                u"nb": nb,
                u"has_next": has_next
            }

        elif request.method == u"PUT":
            count = self._db_api.update(
                table=table,
                filters=filters,
                item=data
            )

            result = {
                u"count": count
            }

        elif request.method == u"DELETE":
            count = self._db_api.delete(
                table=table,
                filters=filters
            )

            result = {
                u"count": count
            }

        elif request.method == u"POST":
            created_id = self._db_api.create(
                table=table,
                item=data
            )

            result = {
                u"id": created_id
            }

            code = 201

        return jsonify(result), code
