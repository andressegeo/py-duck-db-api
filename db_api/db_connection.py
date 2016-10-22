# -*- coding: utf-8 -*-

import datetime
import calendar


class DBConnection(object):

    def __init__(
            self,
            db_api,
            user,
            password,
            database,
            host=u"127.0.0.1"):

        self._db = db_api.connect(
            host=host,
            user=user,
            passwd=password,
            db=database,
            charset=u"utf8"
        )

        self._database = database


    def get_referenced(self, table):
        cursor = self._db.cursor()

        query = u"""
        SELECT
        TABLE_NAME,
        COLUMN_NAME,
        REFERENCED_TABLE_NAME,
        REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = %s
        AND REFERENCED_TABLE_NAME IS NOT NULL
        """

        cursor.execute(query, (self._database, table))

        return [
            (table, constraint[1], constraint[2], constraint[3])
            for constraint in cursor.fetchall()
        ]

    def get_headers(self, table):

        referenced = self.get_referenced(table)
        print(referenced)
        cursor = self._db.cursor()

        query = u"""
        DESCRIBE
        """ + table + """"""

        cursor.execute(query)

        header_to_ignore, foreign_tables = [], []
        if len(referenced) > 0:
            header_to_ignore, foreign_tables = zip(*[(field[1], field[2]) for field in referenced])

        headers = [
            u"`" + table + u"`.`" + row[0] + u"`"
            for row in cursor.fetchall()
            if row[0] not in header_to_ignore
        ]

        for foreign_table in foreign_tables:
            headers += self.get_headers(foreign_table)

        print(headers)
        return headers

    def select(self, table, where=None):
        referenced = self.get_referenced(table)
        headers = self.get_headers(table)

        joins = [
            (u"JOIN " + ref[2] + u" ON `" + ref[0] + u"`.`" + ref[1] + u"` = `" + ref[2] + u"`.`" + ref[3]) + u"`"
            for ref in referenced
        ]

        query = u"""
        SELECT """ + u", ".join(headers) + u"""
        FROM """ + table + u" " + (u" ".join(joins))

        cursor = self._db.cursor()
        cursor.execute(query)

        return headers, cursor.fetchall()