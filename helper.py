# -*- coding: utf-8 -*-

import MySQLdb
from db_api.builder import build_db_api

DB_API = build_db_api(
    db_api_def=MySQLdb,
    db_name=u"hours_count",
    db_password=u"localroot1234",
    db_user=u"root",
    db_host=u"127.0.0.1"
)

result, has_next = DB_API.list(table=u"project", limit=2)
