# -*- coding: utf-8 -*-


import MySQLdb
import json
from db_api.db_connection import DBConnection
from db_api.db_request_parser import DBRequestParser


db_connection = DBConnection(
    db_api=MySQLdb,
    user=u"root",
    password=u"localroot1234",
    database=u"hours_count"
)




# db_parser = DBRequestParser(
#     db_connection=db_connection,
#     table_columns=table_columns,
#     table_relations=table_relations
# )
ret = db_connection.select(u"hour")


print(ret)






