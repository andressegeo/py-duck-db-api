

import pytest
from db_parser import DBParser


@pytest.fixture(scope=u"function")
def mock_referenced():
    return [
        (u'hour', u'project_id', u'project', u'id'),
        (u'hour', u'user_id', u'user', u'id')
    ]


@pytest.fixture(scope=u"function")
def mock_headers():
    return [
        u'`hour`.`id`',
        u'`hour`.`issue`',
        u'`hour`.`started_at`',
        u'`hour`.`minutes`',
        u'`hour`.`comments`',
        u'`project`.`id`',
        u'`project`.`name`',
        u'`user`.`id`',
        u'`user`.`email`',
        u'`user`.`name`'
    ]


@pytest.fixture(scope=u"function")
def db_parser(mock_headers, mock_referenced):
    db_parser = DBParser(
        table=u"hour",
        headers=mock_headers,
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
                        u"issue": u"val 2"
                    }, {
                        u"issue": {
                            u"$eq" : u"val 3"
                        }
                    }
                ]
            }
        ]
    })

    assert ret[u"statements"] == u"(`hour`.`issue` = %s OR (`hour`.`issue` = %s AND `hour`.`issue` = %s))"
    assert ret[u"values"][0] == u"val 1"
    assert ret[u"values"][1] == u"val 2"
    assert ret[u"values"][2] == u"val 3"

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

    ret = db_parser.json_to_formatted_header(u"issue")
    assert ret == u"`hour`.`issue`"

    ret = db_parser.json_to_formatted_header(u"user.email")
    assert ret == u"`user`.`email`"

    ret = db_parser.json_to_formatted_header(u"startedAt")
    assert ret == u"`hour`.`started_at`"
