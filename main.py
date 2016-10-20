# -*- coding: utf-8 -*-


import MySQLdb
import json
from flask import Flask, jsonify

from db_api.db_connection import DBConnection
from db_api.db_parser import DBParser


db_connection = DBConnection(
    db_api=MySQLdb,
    user=u"root",
    password=u"localroot1234",
    database=u"hours_count"
)

db_parser = DBParser()


headers = db_connection.select(u"hour")
print(headers)

# app = Flask(__name__)
#
# @app.route('/api/db/<string:table>')
# def hour(table):
#     db_connection.get_table_columns()
#
# app.run(debug=True)
#






