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
        {u'extra': u'', u'referenced_alias': u'contact', u'referenced_column_name': u'id',
         u'referenced_table_name': u'phone', u'alias': u'user', u'table_name': u'user', u'key': u'mul', u'null': False,
         u'type': u'int(11)', u'column_name': u'contact'},
        {u'extra': u'auto_increment', u'alias': u'company', u'table_name': u'company', u'key': u'pri', u'null': False,
         u'type': u'int(11)', u'column_name': u'id'},
        {u'extra': u'', u'alias': u'contact', u'table_name': u'phone', u'key': u'pri', u'null': False,
         u'type': u'int(11)', u'column_name': u'id'},
        {u'extra': u'', u'alias': u'contact', u'table_name': u'phone', u'key': u'', u'null': False,
         u'type': u'varchar(45)', u'column_name': u'number'},
        {u'extra': u'', u'referenced_alias': u'contact', u'referenced_column_name': u'id',
         u'referenced_table_name': u'phone', u'alias': u'company', u'table_name': u'company', u'key': u'mul',
         u'null': False, u'type': u'int(11)', u'column_name': u'contact'},
        {u'extra': u'', u'referenced_alias': u'company', u'referenced_column_name': u'id',
         u'referenced_table_name': u'company', u'alias': u'user', u'table_name': u'user', u'key': u'mul',
         u'null': False, u'type': u'int(11)', u'column_name': u'company'}]


@pytest.fixture(scope=u"function")
def db_parser(mock_columns):
    db_parser = DBParser(
        table=u"user",
        columns=mock_columns
    )

    return db_parser


def test_generate_base_state(db_parser):
    ret = db_parser.generate_base_state(
        u"user"
    )
    # print(json.dumps(ret, indent=4))


@pytest.fixture(scope=u"function")
def mock_base_state():
    return {u'fields': [{u'alias': u'user.id', u'db': u'`user`.`id`', u'formated': u'id', u"type": u"number"},
                        {u'alias': u'contact.id', u'db': u'`contact`.`id`', u'formated': u'contact.id', u"type": u"number"},
                        {u'alias': u'contact.number', u'db': u'`contact`.`number`', u'formated': u'contact.number', u"type": u"text"},
                        {u'alias': u'company.id', u'db': u'`company`.`id`', u'formated': u'company.id', u"type": u"number"},
                        {u'alias': u'contact.id', u'db': u'`contact`.`id`', u'formated': u'company.contact.id', u"type": u"number"},
                        {u'alias': u'contact.number', u'db': u'`contact`.`number`, u"type": u"text"',
                         u'formated': u'company.contact.number'}], u'type': u'base', u'joins': [
        {u'extra': u'', u'referenced_alias': u'contact', u'referenced_column_name': u'id',
         u'referenced_table_name': u'phone', u'alias': u'user', u'table_name': u'user', u'key': u'mul', u'null': False,
         u'type': u'int(11)', u'column_name': u'contact'},
        {u'extra': u'', u'referenced_alias': u'company', u'referenced_column_name': u'id',
         u'referenced_table_name': u'company', u'alias': u'user', u'table_name': u'user', u'key': u'mul',
         u'null': False, u'type': u'int(11)', u'column_name': u'company'},
        {u'extra': u'', u'referenced_alias': u'contact_0', u'referenced_column_name': u'id',
         u'referenced_table_name': u'phone', u'alias': u'company', u'table_name': u'company', u'key': u'mul',
         u'null': False, u'type': u'int(11)', u'column_name': u'contact'}]}


def test_parse_match(db_parser, mock_base_state):
    base_state = mock_base_state

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

    to_check = [u'`id` = %s', u'`contact.number` = %s']
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

    to_check = [u'`contact.id` = %s', u"AND", u'`contact.id` >= %s']
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

    to_check = [u'`contact.id` = %s', u"OR", u'`company.contact.id` = %s']
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

    to_check = [u'`company.contact.id` = %s', u"OR", u'contact.id` >= %s']
    for val in to_check:
        assert val in ret.get(u"statements", [])

