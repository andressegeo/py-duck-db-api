# -*- coding: utf-8 -*-

import pytest
import json
from db_parser import DBParser


@pytest.fixture(scope=u"function")
def mock_columns():
    return [
        {
            "type": "int(11)",
            "alias": "hour",
            "table_name": "hour",
            "null": False,
            "column_name": "id"
        },
        {
            "type": "varchar(45)",
            "alias": "hour",
            "table_name": "hour",
            "null": False,
            "column_name": "issue"
        },
        {
            "type": "datetime",
            "alias": "hour",
            "table_name": "hour",
            "null": False,
            "column_name": "started_at"
        },
        {
            "type": "int(11)",
            "alias": "hour",
            "table_name": "hour",
            "null": False,
            "column_name": "minutes"
        },
        {
            "type": "varchar(255)",
            "alias": "hour",
            "table_name": "hour",
            "null": True,
            "column_name": "comments"
        },
        {
            "type": "int(11)",
            "alias": "project",
            "table_name": "project",
            "null": False,
            "column_name": "id"
        },
        {
            "type": "varchar(45)",
            "alias": "project",
            "table_name": "project",
            "null": False,
            "column_name": "name"
        },
        {
            "type": "int(11)",
            "alias": "client",
            "table_name": "client",
            "null": False,
            "column_name": "id"
        },
        {
            "type": "varchar(45)",
            "alias": "client",
            "table_name": "client",
            "null": False,
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
            "column_name": "id",
            "null": False,
        },
        {
            "type": "varchar(255)",
            "alias": "affected_to",
            "table_name": "user",
            "column_name": "email",
            "null": False
        },
        {
            "type": "varchar(255)",
            "alias": "affected_to",
            "table_name": "user",
            "column_name": "name",
            "null": False
        },
        {
            "referenced_alias": "affected_to",
            "referenced_column_name": "id",
            "referenced_table_name": "user",
            "alias": "hour",
            "table_name": "hour",
            "type": "int(11)",
            "column_name": "affected_to",
            "null": False
        },
        {
            "type": "int(11)",
            "alias": "created_by",
            "table_name": "user",
            "column_name": "id",
            "null": False
        },
        {
            "type": "varchar(255)",
            "alias": "created_by",
            "table_name": "user",
            "column_name": "email",
            "null": False
        },
        {
            "type": "varchar(255)",
            "alias": "created_by",
            "table_name": "user",
            "column_name": "name",
            "null": False
        },
        {
            "referenced_alias": "created_by",
            "referenced_column_name": "id",
            "referenced_table_name": "user",
            "alias": "hour",
            "table_name": "hour",
            "type": "int(11)",
            "column_name": "created_by",
            "null": False
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
    base_state = db_parser.generate_base_state()
    db_parser._last_state = base_state
    wrapped_values = db_parser.get_wrapped_values(headers=[
            u"hour.issue",
            u"hour.id",
            u"hour.started_at"
        ],
        values=[
            u"test",
            u"test",
            1234
        ]
    )
    assert wrapped_values == u"%s, %s, FROM_UNIXTIME(%s)"

    dummy_state = {
        u"fields": [
            {
                u"alias": u"issue",
            }
        ],
        u"type": u"project"
    }
    db_parser._last_state = dummy_state

    wrapped_values = db_parser.get_wrapped_values(headers=[
        u"issue"
    ],
        values=[
            u"test"
        ]
    )

    assert wrapped_values == u"%s"


def test_parse_match(db_parser):
    base_state = db_parser.generate_base_state()
    ret = db_parser.parse_match({
            u"issue": u"test"
        },
        from_state=base_state
    )

    assert ret[u"statements"] == u"`hour.issue` = %s"
    assert ret[u"values"][0] == u"test"

    ret = db_parser.parse_match({
            u"issue": {
                u"$eq": u"test",
                u"$gte": u"test"
            }
        },
        from_state=base_state)

    assert ret[u"statements"] == u"`hour.issue` = %s AND `hour.issue` >= %s"
    assert ret[u"values"][0] == u"test"

    ret = db_parser.parse_match({
            u"$or": [
                {
                    u"issue": u"val 1"
                }, {
                    u"issue": u"val 2"
                }
            ]
        },
        from_state=base_state)

    assert ret[u"statements"] == u"(`hour.issue` = %s OR `hour.issue` = %s)"
    assert ret[u"values"][0] == u"val 1"
    assert ret[u"values"][1] == u"val 2"

    ret = db_parser.parse_match({
            u"$or": [
                {
                    u"project.client.id": 1
                }, {
                    u"$and": [
                        {
                            u"started_at": {
                                u"$gte": 1477180920
                            }
                        }, {
                            u"affected_to.email": {
                                u"$eq": u"klambert@gpartner.eu"
                            }
                        }
                    ]
                }
            ]
        },
        from_state=base_state)

    assert ret[u"statements"] == u"(`client.id` = %s OR (`hour.started_at` >= FROM_UNIXTIME(%s) AND `affected_to.email` = %s))"
    assert ret[u"values"][0] == 1
    assert ret[u"values"][1] == 1477180920
    assert ret[u"values"][2] == u"klambert@gpartner.eu"


    ret = db_parser.parse_match({
            u"affected_to.id": 1
        },
        from_state=base_state
    )

    assert ret[u"statements"] == u"`affected_to.id` = %s"
    assert ret[u"values"][0] == 1


def test_is_field(db_parser):
    base_state = db_parser.generate_base_state()
    db_parser._last_state = base_state
    ret = db_parser.is_field(u"issue")
    assert ret is True

    ret = db_parser.is_field(u"badField")
    assert ret is False

    ret = db_parser.is_field(u"affected_to.email")
    assert ret is True

    ret = db_parser.is_field(u"started_at")
    assert ret is True


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
            u"affected_to.id": 1,
            u"project.id": 1,
            u"started_at": 1477434540
        }
    })
    assert ret[u"statements"] == u"SET `hour`.`started_at` = FROM_UNIXTIME(%s), `hour`.`project` = %s, `hour`.`affected_to` = %s"
    assert ret[u"values"][1] == 1
    assert ret[u"values"][2] == 1

    ret = db_parser.parse_update({
        u"$set": {
            u"affected_to": {
                u"id": 1,
                u"name": u"Kevin LAMBERT"
            },
            u"project": {
                u"id": 1
            },

        }
    })

    assert ret[u"statements"] == u"SET `hour`.`project` = %s, `hour`.`affected_to` = %s"
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
        u"started_at": 1476057600,
        u"affected_to": {
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

    for column_to_check in columns_to_check:
        assert column_to_check in ret[u"fields"]
    str_vals = u",".join([str(val) for val in ret[u"values"]])

    # Check if values are in array
    for val in (5, 1, 1, u'test', u'test', 1476057600):
        assert val in ret[u"values"]


def test_generate_dependencies(db_parser):
    ret = db_parser.generate_base_state()
    for field in [u"fields", u"joins"]:
        assert field in ret


def test_generate_description(db_parser):

    columns = [
        {
            u"extra": u"auto_increment",
            u"alias": u"project",
            u"table_name": u"project",
            u"key": u"pri",
            u"null": False,
            u"type": u"int(11)",
            u"column_name": u"id"
        },
        {
            u"extra": u"",
            u"alias": u"project",
            u"table_name": u"project",
            u"key": u"",
            u"null": True,
            u"type": u"varchar(45)",
            u"column_name": u"name"
        },
        {
            u"extra": u"auto_increment",
            u"alias": u"client",
            u"table_name": u"client",
            u"key": u"pri",
            u"null": False,
            u"type": u"int(11)",
            u"column_name": u"id"
        },
        {
            u"extra": u"",
            u"alias": u"client",
            u"table_name": u"client",
            u"key": u"",
            u"null": False,
            u"type": u"varchar(45)",
            u"column_name": u"name"
        },
        {
            u"extra": u"",
            u"referenced_alias": u"client",
            u"referenced_column_name": u"id",
            u"referenced_table_name": u"client",
            u"alias": u"project",
            u"table_name": u"project",
            u"key": u"mul",
            u"null": False,
            u"type": u"int(11)",
            u"column_name": u"client"
        },
        {
            u"extra": u"",
            u"alias": u"project",
            u"table_name": u"project",
            u"key": u"",
            u"null": False,
            u"type": u"int(11)",
            u"column_name": u"provisioned_hours"
        }
    ]

    ret = db_parser.generate_column_description(columns=columns, table=u"project")
    print(json.dumps(ret, indent=4))
    print(json.dumps(ret, indent=4))
    assert ret == [
        {
            u"required": True,
            u"type": u"number",
            u"name": u"id",
            u"key": u"pri",
            u"extra": u"auto_increment"
        },
        {
            u"required": False,
            u"type": u"text",
            u"name": u"name"
        },
        {
            u"nestedDescription": {
                u"fields": [
                    {
                        u"required": True,
                        u"type": u"number",
                        u"name": u"id",
                        u"key": u"pri",
                        u"extra": u"auto_increment"
                    },
                    {
                        u"required": True,
                        u"type": u"text",
                        u"name": u"name"
                    }
                ]
            },
            u"required": True,
            u"type": u"number",
            u"name": u"client",
            u"key": u"mul"
        },
        {
            u"required": True,
            u"type": u"number",
            u"name": u"provisioned_hours"
        }
    ]



def test_parse_project(db_parser):

    ret = db_parser.parse_project(project={
            u"id": 1,
            u"issue_formated": u"$issue",
            u"user_email": u"$affected_to.email"
        },
        from_state=db_parser.generate_base_state()
    )
    assert ret[u"statements"] == u"`hour.issue` AS %s, `hour.id` AS %s, `affected_to.email` AS %s"
    assert ret[u'values'] == [u"issue_formated", u"id", u"user_email"]


def test_parse_group(db_parser):
    ret = db_parser.parse_group(group={
            u"_id": {
                u"affected_to": u"$affected_to.id",
                u"project": u"$project.id"
            },
            u"minutes_by_person_and_project": {
                u"$sum": u"$minutes"
            }
        },
        from_state=db_parser.generate_base_state()
    )

    expected_state_fields = [
        {
            u"alias": u"_id.project.id",
            u"formated": u"_id.project.id"
        },
        {
            u"alias": u"_id.affected_to.id",
            u"formated": u"_id.affected_to.id"
        },
        {
            u"alias": u"minutes_by_person_and_project",
            u"formated": u"minutes_by_person_and_project"
        }
    ]

    expected_group_by = [
        u"`project.id`",
        u"`affected_to.id`"
    ]

    expected_values = [
        u"minutes_by_person_and_project"
    ]

    expected_fields = [
        u"`project.id` AS `_id.project.id`",
        u"`affected_to.id` AS `_id.affected_to.id`",
        u"SUM(`hour.minutes`) AS %s"
    ]

    for field in expected_state_fields:
        assert field in ret[u"state"][u"fields"]

    for group in expected_group_by:
        assert group in ret[u"group_by"]

    for value in expected_values:
        assert value in ret[u"values"]

    for field in expected_fields:
        assert field in ret[u"fields"]