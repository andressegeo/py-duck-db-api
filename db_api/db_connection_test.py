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
    pass
    # TODO