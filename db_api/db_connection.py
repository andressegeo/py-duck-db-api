# -*- coding: utf-8 -*-

import datetime
import calendar


class DBConnection(object):

    def __init__(
            self,
            db_api_def,
            user,
            password,
            database,
            host=u"127.0.0.1"):

        self._db = db_api_def.connect(
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

    def get_columns(self, table):

        referenced = self.get_referenced(table)
        cursor = self._db.cursor()

        query = u"""
        DESCRIBE
        """ + table + """"""

        cursor.execute(query)

        header_to_ignore, foreign_tables = [], []
        if len(referenced) > 0:
            header_to_ignore, foreign_tables = zip(*[(field[1], field[2]) for field in referenced])

        columns = [
            ((u"`" + table + u"`.`" + row[0] + u"`"), row[1])
            for row in cursor.fetchall()
            if row[0] not in header_to_ignore
        ]

        for foreign_table in foreign_tables:
            columns += self.get_columns(foreign_table)

        return columns

    def select(self, table, where=None):
        referenced = self.get_referenced(table)
        headers = [column[0] for column in self.get_columns(table)]

        joins = [
            (u"JOIN " + ref[2] + u" ON `" + ref[0] + u"`.`" + ref[1] + u"` = `" + ref[2] + u"`.`" + ref[3]) + u"`"
            for ref in referenced
        ]

        query = u"""
        SELECT """ + u", ".join(headers) + u"""
        FROM """ + table + u" " + (u" ".join(joins))

        if where is not None and where[u"statements"] != u"":
            query = query + u" WHERE " + where[u"statements"]

        cursor = self._db.cursor()
        cursor.execute(query, where[u'values'])

        return headers, cursor.fetchall()

    def update(self, table, update, where):

        if where[u"statements"] != u"":
            where[u"statements"] = u"WHERE " + where[u"statements"]

        query = u"""
        UPDATE """ + table + u""" """ + update[u"statements"] + where[u"statements"]

        cursor = self._db.cursor()

        cursor.execute(u"SELECT COUNT(*) FROM " + table + u" " + where[u"statements"], where[u"values"])
        count = cursor.fetchall()[0][0]

        cursor.execute(query, update[u"values"] + where[u"values"])

        return count
