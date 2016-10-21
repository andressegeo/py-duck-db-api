# -*- coding: utf-8 -*-


import MySQLdb
import json

from flask import Flask, request
from db_api.db_flask_api import DBFlaskAPI
from db_api.db_parser import DBParser
from db_api.db_connection import DBConnection




db_flask_api = DBFlaskAPI(
    db_connection=DBConnection(
        db_api=MySQLdb,
        user=u"root",
        password=u"localroot1234",
        database=u"hours_count"
    ),
    db_parser=DBParser()
)



app = Flask(__name__)

@app.route('/api/db/<string:table>')
def hour(table):
    return db_flask_api.handle_request(request, table=table)

app.run(debug=True)
#






