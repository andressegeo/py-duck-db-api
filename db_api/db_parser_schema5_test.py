# -*- coding: utf-8 -*-

from pytest import fixture
from db_parser import DBParser
import pytest
import json


@fixture(scope=u"function")
def mock_columns():
    return [
        {u'extra': u'', u'alias': u'user', u'table_name': u'user', u'key': u'pri', u'null': False, u'type': u'int(11)',
         u'column_name': u'id'},
        {u'extra': u'', u'alias': u'contact', u'table_name': u'phone', u'key': u'pri', u'null': False,
         u'type': u'int(11)', u'column_name': u'id'},
        {u'extra': u'', u'alias': u'contact', u'table_name': u'phone', u'key': u'', u'null': False,
         u'type': u'varchar(45)', u'column_name': u'number'},
        {u'extra': u'auto_increment', u'alias': u'type', u'table_name': u'type', u'key': u'pri', u'null': False,
         u'type': u'int(11)', u'column_name': u'id'},
        {u'extra': u'', u'alias': u'type', u'table_name': u'type', u'key': u'', u'null': True, u'type': u'varchar(45)',
         u'column_name': u'name'}, {u'extra': u'', u'referenced_alias': u'type', u'referenced_column_name': u'id',
                                    u'referenced_table_name': u'type', u'alias': u'contact', u'table_name': u'phone',
                                    u'key': u'mul', u'null': False, u'type': u'int(11)', u'column_name': u'type'},
        {u'extra': u'', u'referenced_alias': u'contact', u'referenced_column_name': u'id',
         u'referenced_table_name': u'phone', u'alias': u'user', u'table_name': u'user', u'key': u'mul', u'null': False,
         u'type': u'int(11)', u'column_name': u'contact'},
        {u'extra': u'auto_increment', u'alias': u'company', u'table_name': u'company', u'key': u'pri', u'null': False,
         u'type': u'int(11)', u'column_name': u'id'},
        {u'extra': u'', u'alias': u'contact', u'table_name': u'phone', u'key': u'pri', u'null': False,
         u'type': u'int(11)', u'column_name': u'id'},
        {u'extra': u'', u'alias': u'contact', u'table_name': u'phone', u'key': u'', u'null': False,
         u'type': u'varchar(45)', u'column_name': u'number'},
        {u'extra': u'auto_increment', u'alias': u'type', u'table_name': u'type', u'key': u'pri', u'null': False,
         u'type': u'int(11)', u'column_name': u'id'},
        {u'extra': u'', u'alias': u'type', u'table_name': u'type', u'key': u'', u'null': True, u'type': u'varchar(45)',
         u'column_name': u'name'}, {u'extra': u'', u'referenced_alias': u'type', u'referenced_column_name': u'id',
                                    u'referenced_table_name': u'type', u'alias': u'contact', u'table_name': u'phone',
                                    u'key': u'mul', u'null': False, u'type': u'int(11)', u'column_name': u'type'},
        {u'extra': u'', u'referenced_alias': u'contact', u'referenced_column_name': u'id',
         u'referenced_table_name': u'phone', u'alias': u'company', u'table_name': u'company', u'key': u'mul',
         u'null': False, u'type': u'int(11)', u'column_name': u'contact'},
        {u'extra': u'', u'referenced_alias': u'company', u'referenced_column_name': u'id',
         u'referenced_table_name': u'company', u'alias': u'user', u'table_name': u'user', u'key': u'mul',
         u'null': False, u'type': u'int(11)', u'column_name': u'company'},
        {u'extra': u'', u'alias': u'user', u'table_name': u'user', u'key': u'', u'null': False, u'type': u'datetime',
         u'column_name': u'birth'}]


@pytest.fixture(scope=u"function")
def db_parser(mock_columns):
    db_parser = DBParser(
        table=u"user",
        columns=mock_columns
    )

    return db_parser


def test_parse_match(db_parser):
    base_state = db_parser.generate_base_state()

    # Scenario 1
    ret = db_parser.parse_match({
        u"id": 1,
        u"contact.number": u"0169888291"
    },
        from_state=base_state
    )

    to_check = [1, u"0169888291"]
    for val in to_check:
        assert val in ret.get(u"values", [])

    to_check = [u'`user.id` = %s', u'`user.contact.number` = %s']
    for val in to_check:
        assert val in ret.get(u"statements", [])

    # Scenario 2
    ret = db_parser.parse_match({
        u"contact.id": {
            u"$eq": 1,
            u"$gte": 2
        }
    },
        from_state=base_state
    )

    to_check = [1, 2]
    for val in to_check:
        assert val in ret.get(u"values", [])

    to_check = [u'`user.contact.id` = %s', u"AND", u'`user.contact.id` >= %s']
    for val in to_check:
        assert val in ret.get(u"statements", [])

    # Scenario 3
    ret = db_parser.parse_match({
        u"$or": [
            {
                u"contact.id": 3
            }, {
                u"company.contact.id": 4
            }
        ]
    },
        from_state=base_state
    )

    to_check = [3, 4]
    for val in to_check:
        assert val in ret.get(u"values", [])

    to_check = [u'`user.contact.id` = %s', u"OR", u'`user.company.contact.id` = %s']
    for val in to_check:
        assert val in ret.get(u"statements", [])

    # Scenario 4
    ret = db_parser.parse_match({
        u"$or": [
            {
                u"company.contact.id": 1
            }, {
                u"$and": [
                    {
                        u"contact.id": {
                            u"$gte": 0
                        }
                    }, {
                        u"contact.id": {
                            u"$lt": 100
                        }
                    }
                ]
            }
        ]
    },
        from_state=base_state)

    to_check = [1, 0, 100]
    for val in to_check:
        assert val in ret.get(u"values", [])

    to_check = [u'`user.company.contact.id` = %s', u"OR", u'user.contact.id` >= %s']
    for val in to_check:
        assert val in ret.get(u"statements", [])


def test_parse_insert(db_parser):
    ret = db_parser.parse_insert(data={
        u"birth": 562369861,
        u"company": {
            u"id": 1,
        },
        u"contact": {
            u"id": 2,
        }
    })

    fields = [u"`user`.`company`", u"`user`.`contact`", u"`user`.`birth`"]
    positional_values = [u"%s", u"%s", u"FROM_UNIXTIME(%s)"]
    values = [1, 2, 562369861]

    for field in fields:
        assert field in ret.get(u"fields", [])

    for p_value in positional_values:
        assert p_value in ret.get(u"positional_values", [])

    for value in values:
        assert value in ret.get(u"values", [])


def test_parse_update(db_parser):
    ret = db_parser.parse_update({
        u"$set": {
            u"birth": 570629005,
            u"contact": {
                u"id": 1
            }
        }
    })

    values = [
        1,
        570629005
      ]

    statements = [
        u"`user`.`contact` = %s",
        u"`user`.`birth` = FROM_UNIXTIME(%s)"
    ]

    for value in values:
        assert value in ret.get(u"values", [])

    for statment in statements:
        assert statment in ret.get(u"statements", [])


def test_parse_order_by(db_parser):
    ret = db_parser.parse_order_by(order_by={
        u"id": 1,
        u"contact.id": -1
    },
        from_state=db_parser.generate_base_state()
    )


    statements = [u"`user.id` ASC", u"`user.contact.id` DESC"]
    for stat in statements:
        assert stat in ret.get(u"statements")


def test_parse_project(db_parser):

    ret = db_parser.parse_project(project={
            u"id": 1,
            u"the_awesome_contact.name": u"$contact.id"
        },
        from_state=db_parser.generate_base_state()
    )

    fields_to_check = [
        {
            u"path": [
                u"the_awesome_contact"
            ],
            u"type": u"number",
            u"name": u"name"
        },
        {
            u"path": [],
            u"type": u"number",
            u"name": u"id"
        }
    ]

    for field in fields_to_check:
        assert field in ret[u"state"].get(u"fields", [])


def test_parse_group(db_parser):
    ret = db_parser.parse_group(
        group={
            u"_id": {
                u"company": u"$company.contact.id"
            },
            u"ids_sum": {
                u"$sum": u"$contact.id"
            }
        },
        from_state=db_parser.generate_base_state()
    )
    print(json.dumps({
            u"_id": {
                u"company": u"$company.contact.id"
            },
            u"ids_sum": {
                u"$sum": u"$contact.id"
            }
        }, indent=4))
    print(json.dumps(ret, indent=4))

# localhost:5000/api/db/user/aggregation/?pipeline={"_id":{"id":"$id"},"ids_sum":{"$sum":"$contact.id"}}