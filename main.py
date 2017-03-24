# -*- coding: utf-8 -*-

from flask import Flask
from db_api.blueprint import construct_db_api_blueprint
import MySQLdb


app = Flask(__name__)

from db_api.db_connection import DBConnection
#
# conn = DBConnection(
#     db_api_def=MySQLdb,
#     host=u"127.0.0.1",
#     user=u"root",
#     password=u"localroot1234",
#     database=u"dummy_db"
# )

app.register_blueprint(construct_db_api_blueprint(
    db_driver=MySQLdb,
    db_host=u"127.0.0.1",
    db_user=u"root",
    db_passwd=u"localroot1234",
    db_name=u"hours_count_naitways"
), url_prefix=u'/api')

if __name__ == u"__main__":
    app.run(debug=True)
