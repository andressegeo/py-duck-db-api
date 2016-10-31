# -*- coding: utf-8 -*-

import pytest
from db_parser import DBParser



@pytest.fixture(scope=u"function")
def mock_columns():
    return [
        {
            "type": "int(11)",
            "alias": "hour",
            "table_name": "hour",
            "column_name": "id"
        },
        {
            "type": "varchar(45)",
            "alias": "hour",
            "table_name": "hour",
            "column_name": "issue"
        },
        {
            "type": "datetime",
            "alias": "hour",
            "table_name": "hour",
            "column_name": "started_at"
        },
        {
            "type": "int(11)",
            "alias": "hour",
            "table_name": "hour",
            "column_name": "minutes"
        },
        {
            "type": "varchar(255)",
            "alias": "hour",
            "table_name": "hour",
            "column_name": "comments"
        },
        {
            "type": "int(11)",
            "alias": "project_id_project",
            "table_name": "project",
            "column_name": "id"
        },
        {
            "type": "varchar(45)",
            "alias": "project_id_project",
            "table_name": "project",
            "column_name": "name"
        },
        {
            "type": "int(11)",
            "alias": "client_id_client",
            "table_name": "client",
            "column_name": "id"
        },
        {
            "type": "varchar(45)",
            "alias": "client_id_client",
            "table_name": "client",
            "column_name": "name"
        },
        {
            "referenced_alias": "client_id_client",
            "referenced_column_name": "id",
            "referenced_table_name": "client",
            "alias": "project_id_project",
            "table_name": "project",
            "type": "int(11)",
            "column_name": "client_id"
        },
        {
            "referenced_alias": "project_id_project",
            "referenced_column_name": "id",
            "referenced_table_name": "project",
            "alias": "hour",
            "table_name": "hour",
            "type": "int(11)",
            "column_name": "project_id"
        },
        {
            "type": "int(11)",
            "alias": "user_id_user",
            "table_name": "user",
            "column_name": "id"
        },
        {
            "type": "varchar(255)",
            "alias": "user_id_user",
            "table_name": "user",
            "column_name": "email"
        },
        {
            "type": "varchar(255)",
            "alias": "user_id_user",
            "table_name": "user",
            "column_name": "name"
        },
        {
            "referenced_alias": "user_id_user",
            "referenced_column_name": "id",
            "referenced_table_name": "user",
            "alias": "hour",
            "table_name": "hour",
            "type": "int(11)",
            "column_name": "user_id"
        },
        {
            "type": "int(11)",
            "alias": "created_by_user",
            "table_name": "user",
            "column_name": "id"
        },
        {
            "type": "varchar(255)",
            "alias": "created_by_user",
            "table_name": "user",
            "column_name": "email"
        },
        {
            "type": "varchar(255)",
            "alias": "created_by_user",
            "table_name": "user",
            "column_name": "name"
        },
        {
            "referenced_alias": "created_by_user",
            "referenced_column_name": "id",
            "referenced_table_name": "user",
            "alias": "hour",
            "table_name": "hour",
            "type": "int(11)",
            "column_name": "created_by"
        }
    ]










@pytest.fixture(scope=u"function")
def db_parser(mock_columns):
    db_parser = DBParser(
        table=u"hour",
        columns=mock_columns
    )

    return db_parser


def test_to_one_level_json(db_parser):
    transformed = db_parser.to_one_level_json(obj={
        u"issue": u"test issue",
        u"project": {
            u"client": {
                u"id": 1,
                u"name": u"G Cloud"
            },
            u"id": 1,
            u"name": u"Test"
        },
        u"user": {
            u"id": 1,
            u"email": u"klambert@gpartner.eu"
        }
    })
    assert u"project.id" in transformed
    assert u"user.email" in transformed
    assert u"project.client.id" in transformed
    assert u"project.client.name" in transformed
    assert u"issue" in transformed
    assert transformed[u"user.email"] == u"klambert@gpartner.eu"
    assert transformed[u"project.id"] == 1


def test_get_wrapped_values(db_parser):
    wrapped_values = db_parser.get_wrapped_values(headers=[
            u"`hour`.`issue`",
            u"`hour`.`id`",
            u"`hour`.`started_at`"
        ],
        values=[
            u"test",
            u"test",
            1234
        ]
    )
    assert wrapped_values == u"%s, %s, FROM_UNIXTIME(%s)"


    wrapped_values = db_parser.get_wrapped_values(headers=[
        u"`hour`.`issue`",
        u"`hour`.`id`",
        u"`hour`.`started_at`"
    ],
        values=[
            u"test",
            u"test",
            u"2016-10-09"
        ]
    )

    assert wrapped_values == u"%s, %s, %s"


def test_parse_filters(db_parser):
    ret = db_parser.parse_filters({
        u"issue": u"test"
    })

    assert ret[u"statements"] == u"`hour`.`issue` = %s"
    assert ret[u"values"][0] == u"test"

    ret = db_parser.parse_filters({
        u"issue": {
            u"$eq": u"test",
            u"$gte": u"test"
        }
    })

    assert ret[u"statements"] == u"`hour`.`issue` = %s AND `hour`.`issue` >= %s"
    assert ret[u"values"][0] == u"test"

    ret = db_parser.parse_filters({
        u"$or": [
            {
                u"issue": u"val 1"
            }, {
                u"issue": u"val 2"
            }
        ]
    })

    assert ret[u"statements"] == u"(`hour`.`issue` = %s OR `hour`.`issue` = %s)"
    assert ret[u"values"][0] == u"val 1"
    assert ret[u"values"][1] == u"val 2"

    ret = db_parser.parse_filters({
        u"$or": [
            {
                u"project.client.id": 1
            }, {
                u"$and": [
                    {
                        u"startedAt": {
                            u"$gte": 1477180920
                        }
                    }, {
                        u"user.email": {
                            u"$eq": u"klambert@gpartner.eu"
                        }
                    }
                ]
            }
        ]
    })

    import json
    print(json.dumps(ret, indent=4))
    assert ret[u"statements"] == u"(`client_id_client`.`id` = %s OR (`hour`.`started_at` >= FROM_UNIXTIME(%s) AND `user_id_user`.`email` = %s))"
    assert ret[u"values"][0] == 1
    assert ret[u"values"][1] == 1477180920
    assert ret[u"values"][2] == u"klambert@gpartner.eu"


    ret = db_parser.parse_filters({
        u"user.id": 1
    })

    assert ret[u"statements"] == u"`user_id_user`.`id` = %s"
    assert ret[u"values"][0] == 1


def test_is_field(db_parser):

    ret = db_parser.is_field(u"issue")
    assert ret is True

    ret = db_parser.is_field(u"badField")
    assert ret is False

    ret = db_parser.is_field(u"user.email")
    assert ret is True

    ret = db_parser.is_field(u"startedAt")
    assert ret is True


def test_get_json_for_formatted_header(db_parser):

    ret = db_parser.formated_to_header(u"issue")
    assert ret == u"`hour`.`issue`"

    ret = db_parser.formated_to_header(u"user.email")
    assert ret == u"`user_id_user`.`email`"

    ret = db_parser.formated_to_header(u"startedAt")
    assert ret == u"`hour`.`started_at`"


def test_parse_update(db_parser):

    ret = db_parser.parse_update({
        u"$set": {
            u"comments": u"updated comment",
            u"issue": u"updated issue"
        }
    })

    assert ret[u"statements"] == u"SET `hour`.`issue` = %s, `hour`.`comments` = %s"
    assert ret[u"values"][1] == u"updated comment"
    assert ret[u"values"][0] == u"updated issue"

    ret = db_parser.parse_update({
        u"$set": {
            u"user.id": 1,
            u"project.id": 1,
            u"startedAt" : 1477434540
        }
    })

    assert ret[u"statements"] == u"SET `hour`.`user_id` = %s, `hour`.`project_id` = %s, `hour`.`started_at` = FROM_UNIXTIME(%s)"
    assert ret[u"values"][0] == 1
    assert ret[u"values"][1] == 1

    ret = db_parser.parse_update({
        u"$set": {
            u"user": {
                u"id": 1,
                u"name" : "Kevin LAMBERT"
            },
            u"project": {
                u"id": 1
            },

        }
    })

    assert ret[u"statements"] == u"SET `hour`.`user_id` = %s, `hour`.`project_id` = %s"
    assert ret[u"values"][0] == 1
    assert ret[u"values"][1] == 1

def test_parse_insert(db_parser):

    ret = db_parser.parse_insert(data={
        u"comments": u"test",
        u"issue": u"test",
        u"minutes": 5,
        u"project": {
            u"id": 1,
            u"name": u"Interne"
        },
        u"startedAt": 1476057600,
        u"user": {
            u"email": u"klambert@gpartner.eu",
            u"id": 1,
            u"name": u"Kévin LAMBERT"
        }
    })

    assert ret[u"statements"] == u" ".join([
        u"INSERT INTO",
        u"`hour`(`hour`.`project_id`, `hour`.`started_at`, `hour`.`user_id`, `hour`.`minutes`, `hour`.`issue`, `hour`.`comments`)",
        u"VALUES(%s, FROM_UNIXTIME(%s), %s, %s, %s, %s)"
    ])

    assert ret[u"values"][0] == 1
    assert ret[u"values"][4] == u"test"
    assert ret[u"values"][5] == u"test"


def test_generate_dependencies(db_parser):

    ret = db_parser.generate_dependencies()

    import json
    print(json.dumps(ret, indent=4))
    assert len(ret[0]) == 21
    assert ret[1] == u"hour"
    assert len(ret[2]) == 4
    # import json
    # print(json.dumps(ret, indent=4))