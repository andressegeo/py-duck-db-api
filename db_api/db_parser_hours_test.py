# -*- coding: utf-8 -*-

import pytest
import json
from db_parser import DBParser


def values_in_tab(values, tab):
    for value in values:
        assert value in tab


def check_values_and_statements(ret, values, statements):
    values_in_tab(values, ret.get(u"values", []))
    values_in_tab(statements, ret.get(u"statements", []))


@pytest.fixture(scope=u"function")
def mock_columns_hours():
    return [{u'extra': u'auto_increment', u'alias': u'hour', u'table_name': u'hour', u'key': u'pri', u'null': False,
             u'type': u'int(11)', u'column_name': u'id'},
            {u'extra': u'', u'alias': u'hour', u'table_name': u'hour', u'key': u'', u'null': True, u'type': u'text',
             u'column_name': u'issue'},
            {u'extra': u'', u'alias': u'hour', u'table_name': u'hour', u'key': u'', u'null': False,
             u'type': u'datetime', u'column_name': u'started_at'},
            {u'extra': u'', u'alias': u'hour', u'table_name': u'hour', u'key': u'', u'null': False, u'type': u'int(11)',
             u'column_name': u'minutes'},
            {u'extra': u'', u'alias': u'hour', u'table_name': u'hour', u'key': u'', u'null': True,
             u'type': u'varchar(255)', u'column_name': u'comments'},
            {u'extra': u'auto_increment', u'alias': u'project', u'table_name': u'project', u'key': u'pri',
             u'null': False, u'type': u'int(11)', u'column_name': u'id'},
            {u'extra': u'', u'alias': u'project', u'table_name': u'project', u'key': u'', u'null': False,
             u'type': u'varchar(45)', u'column_name': u'name'},
            {u'extra': u'auto_increment', u'alias': u'client', u'table_name': u'client', u'key': u'pri', u'null': False,
             u'type': u'int(11)', u'column_name': u'id'},
            {u'extra': u'', u'alias': u'client', u'table_name': u'client', u'key': u'', u'null': False,
             u'type': u'varchar(45)', u'column_name': u'name'},
            {u'extra': u'', u'referenced_alias': u'client', u'referenced_column_name': u'id',
             u'referenced_table_name': u'client', u'alias': u'project', u'table_name': u'project', u'key': u'mul',
             u'null': False, u'type': u'int(11)', u'column_name': u'client'},
            {u'extra': u'', u'alias': u'project', u'table_name': u'project', u'key': u'', u'null': False,
             u'type': u'int(11)', u'column_name': u'provisioned_hours'},
            {u'extra': u'', u'alias': u'project', u'table_name': u'project', u'key': u'', u'null': True,
             u'type': u'datetime', u'column_name': u'started_at'},
            {u'extra': u'', u'referenced_alias': u'project', u'referenced_column_name': u'id',
             u'referenced_table_name': u'project', u'alias': u'hour', u'table_name': u'hour', u'key': u'mul',
             u'null': False, u'type': u'int(11)', u'column_name': u'project'},
            {u'extra': u'auto_increment', u'alias': u'affected_to', u'table_name': u'user', u'key': u'pri',
             u'null': False, u'type': u'int(11)', u'column_name': u'id'},
            {u'extra': u'', u'alias': u'affected_to', u'table_name': u'user', u'key': u'', u'null': False,
             u'type': u'varchar(255)', u'column_name': u'email'},
            {u'extra': u'', u'alias': u'affected_to', u'table_name': u'user', u'key': u'', u'null': False,
             u'type': u'varchar(255)', u'column_name': u'name'},
            {u'extra': u'', u'referenced_alias': u'affected_to', u'referenced_column_name': u'id',
             u'referenced_table_name': u'user', u'alias': u'hour', u'table_name': u'hour', u'key': u'mul',
             u'null': False, u'type': u'int(11)', u'column_name': u'affected_to'}]


@pytest.fixture(scope=u"function")
def db_parser(mock_columns_hours):
    db_parser = DBParser(
        table=u"hour",
        columns=mock_columns_hours
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

    stmt_to_check = [u"`hour.project.client.id` = %s", u"`hour.started_at` >= FROM_UNIXTIME(%s)",
                     u"`hour.affected_to.email` = %s"]
    for stmt in stmt_to_check:
        assert stmt in ret[u"statements"]

    values_to_check = [1, 1477180920, u"klambert@gpartner.eu"]
    for value in values_to_check:
        assert value in ret[u"values"]

    ret = db_parser.parse_match({
        u"affected_to.id": 1
    },
        from_state=base_state
    )

    assert ret[u"statements"] == u"`hour.affected_to.id` = %s"
    assert ret[u"values"][0] == 1


def test_parse_update(db_parser):
    ret = db_parser.parse_update({
        u"$set": {
            u"comments": u"updated comment",
            u"issue": u"updated issue"
        }
    })
    check_values_and_statements(
        ret=ret,
        values=[
            u"updated comment",
            u"updated issue"
        ],
        statements=[
            u"`hour`.`issue` = %s",
            u"`hour`.`comments` = %s"
        ]

    )

    ret = db_parser.parse_update({
        u"$set": {
            u"affected_to.id": 1,
            u"project.id": 1,
            u"started_at": 1477434540
        }
    })

    check_values_and_statements(
        ret=ret,
        values=[1, 1477434540, 1],
        statements=[
            u"`hour`.`affected_to` = %s",
            u"`hour`.`started_at` = FROM_UNIXTIME(%s)",
            u"`hour`.`project` = %s"
        ]
    )

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

    check_values_and_statements(
        ret=ret,
        values=[1, 1],
        statements=[
            u"`hour`.`affected_to` = %s",
            u"`hour`.`affected_to` = %s",
            u"`hour`.`project` = %s"
        ]

    )


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

    values_in_tab([
        u"`hour`.`started_at`",
        u"`hour`.`project`",
        u"`hour`.`affected_to`",
        u"`hour`.`minutes`",
        u"`hour`.`issue`",
        u"`hour`.`comments`"
    ], ret[u"fields"])

    values_in_tab(
        [
            u"FROM_UNIXTIME(%s)",
            u"%s",
            u"%s",
            u"%s",
            u"%s",
            u"%s"
        ],
        ret[u"positional_values"]
    )

    values_in_tab([
        1476057600,
        u"Interne",
        1,
        1,
        5,
        u"test",
        u"test",
    ],
        ret[u"values"]
    )


def test_generate_description(db_parser, mock_columns_hours):
    ret = db_parser.generate_column_description(columns=[
        {
            u'extra': u'auto_increment',
            u'alias': u'hour',
            u'table_name': u'hour',
            u'key': u'pri',
            u'null': False,
            u'type': u'int(11)',
            u'column_name': u'id'
        }
    ], table=u"hour")

    assert ret[0][u"required"] == True
    assert ret[0][u"type"] == u"number"
    assert ret[0][u"extra"] == u"auto_increment"
    assert ret[0][u"name"] == u"id"

    ret = db_parser.generate_column_description(columns=[
        {u'extra': u'', u'alias': u'affected_to', u'table_name': u'user', u'key': u'', u'null': False,
         u'type': u'varchar(255)', u'column_name': u'name'},
        {u'extra': u'', u'referenced_alias': u'affected_to', u'referenced_column_name': u'id',
         u'referenced_table_name': u'user', u'alias': u'hour', u'table_name': u'hour', u'key': u'mul',
         u'null': False, u'type': u'int(11)', u'column_name': u'affected_to'}], table=u"hour")

    assert ret[0][u"nestedDescription"][u"source"] == u"user"
    assert ret[0][u"nestedDescription"][u"fields"][0][u"name"] == u"name"


def test_parse_project(db_parser):
    ret = db_parser.parse_project(project={
        u"id": 1,
        u"issue_formatted": u"$issue",
        u"user_email": u"$affected_to.email"
    },
        from_state=db_parser.generate_base_state()
    )

    assert u"state" in ret

    values_in_tab([
        u"issue_formatted",
        u"id",
        u"user_email"
    ], ret[u"values"])

    assert ret[u"statements"] == u"`hour.id` AS %s, `hour.affected_to.email` AS %s, `hour.issue` AS %s"


def test_parse_order_by(db_parser):
    ret = db_parser.parse_order_by(order_by={
        u"affected_to.email": 1,
        u"minutes": -1
    },
        from_state=db_parser.generate_base_state()
    )

    assert ret[u"statements"] == u"`hour.affected_to.email` ASC, `hour.minutes` DESC"


def test_parse_group(db_parser):
    ret = db_parser.parse_group(
        group={
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
            u"path": [
                u"_id"
            ],
            u"type": u"number",
            u"name": u"project"
        },
        {
            u"path": [
                u"_id"
            ],
            u"type": u"number",
            u"name": u"affected_to"
        },
        {
            u"path": [],
            u"type": u"number",
            u"name": u"minutes_by_person_and_project"
        }
    ]

    expected_group_by = [
        u"`hour.project.id`",
        u"`hour.affected_to.id`"
    ]
    expected_values = [
        u"minutes_by_person_and_project"
    ]

    expected_fields = [
        u"`hour.project.id` AS `_id.project`",
        u"`hour.affected_to.id` AS `_id.affected_to`",
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
