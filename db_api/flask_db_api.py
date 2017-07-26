# -*- coding: utf-8 -*-


from flask import jsonify, make_response, Blueprint, request
from collections import OrderedDict
import json


class FlaskDBApi(object):

    def __init__(
            self,
            db_api
    ):
        self._db_api = db_api

    def construct_blueprint(self):

        db_api_blueprint = Blueprint(u'db_api', __name__)

        @db_api_blueprint.route(u'/<string:table>/export', methods=[u"GET"])
        def table_aggregation_export(table):
            return self.handle_export(request, table=table)

        @db_api_blueprint.route(u'/<string:table>', methods=[u"POST", u"PUT", u"DELETE", u"GET"])
        def table_request(table):
            return self.handle_request(request, table=table)

        @db_api_blueprint.route(u'/<string:table>/aggregation', methods=[u"GET", u"POST"])
        def table_aggregation(table):
            return self.handle_aggregation(request, table=table)

        @db_api_blueprint.route(u'/<string:table>/description', methods=[u"GET"])
        def table_description_request(table):
            return self.handle_description(request, table=table)

        return db_api_blueprint

    def handle_export(self, request, table):
        code = 200

        params = {
            u"pipeline": None,
            u"filters": None
        }

        for key in params:
            var = request.args.get(key, None)
            if var is not None:
                params[key] = json.loads(var, encoding=u"utf-8")

        params[u"table"] = table
        headers, rows = self._db_api.export(**params)

        export = self._db_api.export_to_csv(headers, rows, {})
        output = make_response(export.getvalue())
        output.headers[U"Content-Disposition"] = U"attachment; filename=export.csv"
        output.headers[U"Content-type"] = U"text/csv"
        return output, code

    def handle_aggregation(self, request, table):
        code = 200
        # Get pipeline from payload or url arg
        data = request.data

        if data is not None and data != u"":
            data = json.loads(data, encoding=u"utf-8")
        else:
            data = { u"pipeline": [] }
        pipeline = json.loads(request.args.get(u'pipeline', u"[]")) or data.get(u"pipeline", [])

        result = self._db_api.aggregate(table, pipeline)
        return jsonify(result), code

    def handle_description(self, request, table):
        fields = self._db_api.description(table)
        return jsonify({
            u"fields": fields
        }), 200

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
