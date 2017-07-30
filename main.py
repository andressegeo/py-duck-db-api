# -*- coding: utf-8 -*-

from flask import Flask
from db_api.builder import build_db_api
from db_api.flask_db_api import FlaskDBApi
import MySQLdb


APP = Flask(__name__)

DB_API = build_db_api(
    db_api_def=MySQLdb,
    db_name=u"hours_count",
    db_password=u"localroot1234",
    db_user=u"root",
    db_host=u"127.0.0.1"
)

db_flask_api = FlaskDBApi(DB_API)
db_blueprint = db_flask_api.construct_blueprint()
APP.register_blueprint(db_blueprint, url_prefix=u'/api/db')

if __name__ == u"__main__":
    APP.run(debug=True)
