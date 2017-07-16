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

created_id = DB_API.create(u"client", {
     u"name": u"pouet"
})

count = DB_API.update(u"client", filters={
    u"id": 19
}, item={
    u"$set": {
        u"name": u"pouet1234"
    }
})

count = DB_API.delete(u"client", filters={
    u"name": {
        u"$regex": u"pouet"
    }
})


hour_description = DB_API.description(u"hour")

result = DB_API.aggregate(u"project", pipeline=[{
    u"$project": {
        u"name": u"$name"
    }
}])
print(result)
