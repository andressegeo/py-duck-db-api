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
            "alias": "project",
            "table_name": "project",
            "column_name": "id"
        },
        {
            "type": "varchar(45)",
            "alias": "project",
            "table_name": "project",
            "column_name": "name"
        },
        {
            "type": "int(11)",
            "alias": "client",
            "table_name": "client",
            "column_name": "id"
        },
        {
            "type": "varchar(45)",
            "alias": "client",
            "table_name": "client",
            "column_name": "name"
        },
        {
            "referenced_alias": "client",
            "referenced_column_name": "id",
            "referenced_table_name": "client",
            "alias": "project",
            "table_name": "project",
            "type": "int(11)",
            "column_name": "client"
        },
        {
            "referenced_alias": "project",
            "referenced_column_name": "id",
            "referenced_table_name": "project",
            "alias": "hour",
            "table_name": "hour",
            "type": "int(11)",
            "column_name": "project"
        },
        {
            "type": "int(11)",
            "alias": "affected_to",
            "table_name": "user",
            "column_name": "id"
        },
        {
            "type": "varchar(255)",
            "alias": "affected_to",
            "table_name": "user",
            "column_name": "email"
        },
        {
            "type": "varchar(255)",
            "alias": "affected_to",
            "table_name": "user",
            "column_name": "name"
        },
        {
            "referenced_alias": "affected_to",
            "referenced_column_name": "id",
            "referenced_table_name": "user",
            "alias": "hour",
            "table_name": "hour",
            "type": "int(11)",
            "column_name": "affected_to"
        },
        {
            "type": "int(11)",
            "alias": "created_by",
            "table_name": "user",
            "column_name": "id"
        },
        {
            "type": "varchar(255)",
            "alias": "created_by",
            "table_name": "user",
            "column_name": "email"
        },
        {
            "type": "varchar(255)",
            "alias": "created_by",
            "table_name": "user",
            "column_name": "name"
        },
        {
            "referenced_alias": "created_by",
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
                        u"affectedTo.email": {
                            u"$eq": u"klambert@gpartner.eu"
                        }
                    }
                ]
            }
        ]
    })

    assert ret[u"statements"] == u"(`client`.`id` = %s OR (`hour`.`started_at` >= FROM_UNIXTIME(%s) AND `affected_to`.`email` = %s))"
    assert ret[u"values"][0] == 1
    assert ret[u"values"][1] == 1477180920
    assert ret[u"values"][2] == u"klambert@gpartner.eu"


    ret = db_parser.parse_filters({
        u"affectedTo.id": 1
    })

    assert ret[u"statements"] == u"`affected_to`.`id` = %s"
    assert ret[u"values"][0] == 1


def test_is_field(db_parser):

    ret = db_parser.is_field(u"issue")
    assert ret is True

    ret = db_parser.is_field(u"badField")
    assert ret is False

    ret = db_parser.is_field(u"affectedTo.email")
    assert ret is True

    ret = db_parser.is_field(u"startedAt")
    assert ret is True


def test_get_json_for_formatted_header(db_parser):

    ret = db_parser.formated_to_header(u"issue")
    assert ret == u"`hour`.`issue`"

    ret = db_parser.formated_to_header(u"affectedTo.email")
    assert ret == u"`affected_to`.`email`"

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
            u"affectedTo.id": 1,
            u"project.id": 1,
            u"startedAt": 1477434540
        }
    })

    assert ret[u"statements"] == u"SET `hour`.`affected_to` = %s, `hour`.`project` = %s, `hour`.`started_at` = FROM_UNIXTIME(%s)"
    assert ret[u"values"][0] == 1
    assert ret[u"values"][1] == 1

    ret = db_parser.parse_update({
        u"$set": {
            u"affectedTo": {
                u"id": 1,
                u"name" : "Kevin LAMBERT"
            },
            u"project": {
                u"id": 1
            },

        }
    })

    assert ret[u"statements"] == u"SET `hour`.`affected_to` = %s, `hour`.`project` = %s"
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
        u"affectedTo": {
            u"email": u"klambert@gpartner.eu",
            u"id": 1,
            u"name": u"Kévin LAMBERT"
        }
    })

    columns_to_check = [
        u"`hour`.`project`",
        u"`hour`.`started_at`",
        u"`hour`.`affected_to`",
        u"`hour`.`minutes`",
        u"`hour`.`issue`",
        u"`hour`.`comments`"
    ]

    assert u"INSERT INTO `hour`" in ret[u"statements"]
    for column_to_check in columns_to_check:
        assert column_to_check in ret[u"statements"]
    str_vals = u",".join([str(val) for val in ret[u"values"]])

    # Check if values are in array
    for val in (5, 1, 1, u'test', u'test', 1476057600):
        assert str(val) in str_vals


def test_generate_dependencies(db_parser):

    ret = db_parser.generate_dependencies()
    assert len(ret[0]) == 15
    assert ret[1] == u"hour"
    assert len(ret[2]) == 4