

import pytest
from db_parser import DBParser

@pytest.fixture(scope=u"function")
def db_parser(self):

    db_parser = DBParser()
    return db_parser


def test_parse_filters(self, db_parser):

    ret = db_parser.parse_filters({
        u"issue": {
            u"$eq": u"test"
        }
    })

    assert ret[u"statements"] == u"`hour`.`issue` = %s"
    assert ret[u"values"][0] == u"test"
