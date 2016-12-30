# -*- coding: utf-8 -*-

import pytest
import json
from mock import Mock
from db_connection import DBConnection
from db_parser import DBParser

@pytest.fixture(scope=u"function")
def mock_base_dependency():
    return [
        [
            {
                "alias": "`hour.id`", 
                "db": "`hour`.`id`", 
                "formated": "id"
            }, 
            {
                "alias": "`hour.issue`", 
                "db": "`hour`.`issue`", 
                "formated": "issue"
            }, 
            {
                "alias": "`hour.started_at`", 
                "db": "`hour`.`started_at`", 
                "formated": "startedAt"
            }, 
            {
                "alias": "`hour.minutes`", 
                "db": "`hour`.`minutes`", 
                "formated": "minutes"
            }, 
            {
                "alias": "`hour.comments`", 
                "db": "`hour`.`comments`", 
                "formated": "comments"
            }, 
            {
                "alias": "`project.id`", 
                "db": "`project`.`id`", 
                "formated": "project.id"
            }, 
            {
                "alias": "`project.name`", 
                "db": "`project`.`name`", 
                "formated": "project.name"
            }, 
            {
                "alias": "`client.id`", 
                "db": "`client`.`id`", 
                "formated": "project.client.id"
            }, 
            {
                "alias": "`client.name`", 
                "db": "`client`.`name`", 
                "formated": "project.client.name"
            }, 
            {
                "alias": "`affected_to.id`", 
                "db": "`affected_to`.`id`", 
                "formated": "affectedTo.id"
            }, 
            {
                "alias": "`affected_to.email`", 
                "db": "`affected_to`.`email`", 
                "formated": "affectedTo.email"
            }, 
            {
                "alias": "`affected_to.name`", 
                "db": "`affected_to`.`name`", 
                "formated": "affectedTo.name"
            }
        ], 
        "hour", 
        [
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
                "referenced_alias": "client", 
                "referenced_column_name": "id", 
                "referenced_table_name": "client", 
                "alias": "project", 
                "table_name": "project", 
                "type": "int(11)", 
                "column_name": "client"
            }, 
            {
                "referenced_alias": "affected_to", 
                "referenced_column_name": "id", 
                "referenced_table_name": "user", 
                "alias": "hour", 
                "table_name": "hour", 
                "type": "int(11)", 
                "column_name": "affected_to"
            }
        ], 
        {
            "values": [
                5
            ], 
            "statements": "`hour`.`id` = %s"
        }
    ]

    
@pytest.fixture(scope=u"function")
def mock_filter_dependency_0():
    return {
        u"values": [
            11
        ], 
        u"statements": u"(`project.id` = %s)"
    }


@pytest.fixture(scope=u"function")
def mock_filter_dependency_1():
    return {
        u"values": [
            1, 
            1477180920, 
            u"klambert@gpartner.eu"
        ], 
        u"statements": u"(`client.id` = %s OR (`hour.started_at` >= FROM_UNIXTIME(%s) AND `affected_to.email` = %s))"
    }


@pytest.fixture(scope=u"function")
def fake_mysql_db():
    mysqldb = Mock()
    mysqldb.connect = Mock()

    return mysqldb


@pytest.fixture(scope=u"function")
def mock_db_connection(fake_mysql_db):
    db_connection = DBConnection(
        db_api_def=fake_mysql_db,
        user=u"user",
        password=u"password",
        database=u"database"
    )
    return db_connection


@pytest.fixture(scope=u"function")
def mock_db_parser():
    db_connection = DBParser(
        table=u"hours",
        columns=[]
    )
    return db_connection


def test_aggregate(
        mock_db_parser,
        mock_db_connection,
        mock_base_dependency,
        mock_filter_dependency_0,
        mock_filter_dependency_1
):
    # Return whatever, we don't test that, only the SQL request
    mock_db_connection._execute = Mock(return_value=[[]])

    # Do the aggregation
    ret = mock_db_connection.aggregate(
        base_state=mock_base_dependency,
        formater=mock_db_parser.rows_to_formated,
        stages=[
            ("$match", mock_filter_dependency_0),
            ("$match", mock_filter_dependency_1)
        ]
    )

    call_args = mock_db_connection._execute.call_args
    assert call_args[0][
               0] == u"SELECT * FROM " \
                     u"( " \
                     u"SELECT * " \
                     u"FROM " \
                     u"( " \
                     u"SELECT " \
                     u"`hour`.`id` AS `hour.id`, " \
                     u"`hour`.`issue` AS `hour.issue`, " \
                     u"`hour`.`started_at` AS `hour.started_at`, " \
                     u"`hour`.`minutes` AS `hour.minutes`, " \
                     u"`hour`.`comments` AS `hour.comments`, " \
                     u"`project`.`id` AS `project.id`, " \
                     u"`project`.`name` AS `project.name`," \
                     u" `client`.`id` AS `client.id`, " \
                     u"`client`.`name` AS `client.name`, " \
                     u"`affected_to`.`id` AS `affected_to.id`, " \
                     u"`affected_to`.`email` AS `affected_to.email`, " \
                     u"`affected_to`.`name` AS `affected_to.name` " \
                     u"FROM hour " \
                     u"JOIN `project` AS `project` ON `hour`.`project` = `project`.`id` " \
                     u"JOIN `client` AS `client` ON `project`.`client` = `client`.`id` " \
                     u"JOIN `user` AS `affected_to` ON `hour`.`affected_to` = `affected_to`.`id` " \
                     u") AS s_0 " \
                     u"WHERE (`project.id` = %s) " \
                     u") AS s_1 " \
                     u"WHERE " \
                     u"(`client.id` = %s OR (`hour.started_at` >= FROM_UNIXTIME(%s) AND `affected_to.email` = %s))"
    assert call_args[0][1] == [11, 1, 1477180920, u'klambert@gpartner.eu']
