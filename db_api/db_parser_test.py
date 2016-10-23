# -*- coding: utf-8 -*-

import pytest
from db_parser import DBParser


@pytest.fixture(scope=u"function")
def mock_referenced():
    return [
        (u'hour', u'project_id', u'project', u'id'),
        (u'hour', u'user_id', u'user', u'id')
    ]


@pytest.fixture(scope=u"function")
def mock_columns():
    return [
        (u'`hour`.`id`', u'int(11)'),
        (u'`hour`.`issue`', u'varchar(45)'),
        (u'`hour`.`started_at`', u'datetime'),
        (u'`hour`.`minutes`', u'int(11)'),
        (u'`hour`.`comments`', u'varchar(255)'),
        (u'`project`.`id`', u'int(11)'),
        (u'`project`.`name`', u'varchar(45)'),
        (u'`user`.`id`', u'int(11)'),
        (u'`user`.`email`', u'varchar(255)'),
        (u'`user`.`name`', u'varchar(255)')
    ]



@pytest.fixture(scope=u"function")
def db_parser(mock_columns, mock_referenced):
    db_parser = DBParser(
        table=u"hour",
        columns=mock_columns,
        referenced=mock_referenced
    )

    return db_parser


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
                u"issue": u"val 1"
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

    assert ret[u"statements"] == u"(`hour`.`issue` = %s OR (`hour`.`started_at` >= %s AND `user`.`email` = %s))"
    assert ret[u"values"][0] == u"val 1"
    assert ret[u"values"][1] == 1477180920
    assert ret[u"values"][2] == u"klambert@gpartner.eu"

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

    ret = db_parser.json_to_header(u"issue")
    assert ret == u"`hour`.`issue`"

    ret = db_parser.json_to_header(u"user.email")
    assert ret == u"`user`.`email`"

    ret = db_parser.json_to_header(u"startedAt")
    assert ret == u"`hour`.`started_at`"


def test_parse_update(db_parser):

    ret = db_parser.parse_update({
        u"$set": {
            u"comment": u"updated comment",
            u"issue": u"updated issue"
        }
    })

    assert ret[u"statements"] == u"SET `hour`.`comment` = %s, `hour`.`issue` = %s"
    assert ret[u"values"][0] == u"updated comment"
    assert ret[u"values"][1] == u"updated issue"

    ret = db_parser.parse_update({
        u"$set": {
            u"user.id": 1,
            u"project.id": 1
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


def test_to_one_level_json(db_parser):
    transformed = db_parser.to_one_level_json(obj={
        u"issue": u"test issue",
        u"project": {
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
