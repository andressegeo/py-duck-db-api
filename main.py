# -*- coding: utf-8 -*-

from flask import Flask
from db_api.blueprint import construct_db_api_blueprint
import MySQLdb

app = Flask(__name__)

app.register_blueprint(construct_db_api_blueprint(
    db_driver=MySQLdb,
    db_host=u"127.0.0.1",
    db_user=u"root",
    db_passwd=u"localroot1234",
    db_name=u"hours_count"
), url_prefix=u'/api')

if __name__ == u"__main__":
    app.run()

