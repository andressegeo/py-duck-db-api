# -*- coding: utf-8 -*-


import MySQLdb
import json

from flask import Flask, request
from db_api.db_flask_api import DBFlaskAPI
from db_api.db_parser import DBParser
from db_api.db_connection import DBConnection

db_flask_api = DBFlaskAPI(
    db_api_def=MySQLdb,
    db_user=u"root",
    db_password=u"localroot1234",
    db_name=u"hours_count",
    db_connection_def=DBConnection,
    db_parser_def=DBParser
)

app = Flask(__name__)


@app.route('/api/db/<string:table>', methods=[u"POST", u"PUT", u"DELETE", u"GET"])
def hour(table):
    return db_flask_api.handle_request(request, table=table)

app.run(debug=True)






