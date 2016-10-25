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

        self._db_api_def = db_api_def

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
            {
                u"table_name": constraint[0],
                u"column_name": constraint[1],
                u"referenced_table_name": constraint[2],
                u"referenced_column_name": constraint[3]
            }
            for constraint in cursor.fetchall()
        ]

    def get_columns(self, table):

        referenced = self.get_referenced(table)
        cursor = self._db.cursor()

        query = u"""
        DESCRIBE
        """ + table + """"""

        cursor.execute(query)

        columns = []
        # For each row
        for row in cursor.fetchall():
            column = {
                u"table_name": table,
                u"column_name": row[0],
                u"type": row[1]
            }
            # If reference found, add it
            for ref in referenced:
                if (ref[u"table_name"] == column[u"table_name"]
                    and ref[u"column_name"] == column[u"column_name"]):
                    column.update(ref)
                    columns += self.get_columns(ref[u"referenced_table_name"])
                    break
            columns.append(column)

        return columns

    def select(self, table, where=None):
        referenced = self.get_referenced(table)
        headers = [
            u"`" + col[u"table_name"] + u"`.`" + col[u"column_name"] + u"`"
            for col in self.get_columns(table)
        ]

        joins = [
            (
                u"JOIN " + ref[u"referenced_table_name"] + u" ON `"
                + ref[u"table_name"]
                + u"`.`" + ref[u"column_name"] + u"` = `"
                + ref[u"referenced_table_name"]
                + u"`.`" + ref[u"referenced_column_name"] + u"`"
            )
            for ref in referenced
            if (ref[u"table_name"] == table and u"referenced_table_name" in ref)
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
            where[u"statements"] = u" WHERE " + where[u"statements"]

        query = u"""UPDATE """ + table + u""" """ + update[u"statements"] + where[u"statements"]

        cursor = self._db.cursor()

        cursor.execute(u"SELECT COUNT(*) FROM " + table + u" " + where[u"statements"], where[u"values"])
        count = cursor.fetchall()[0][0]

        print(query)
        cursor.execute(query, update[u"values"] + where[u"values"])
        cursor.connection.commit()

        return count

    def delete(self, table, where):

        if where[u"statements"] != u"":
            where[u"statements"] = u"WHERE " + where[u"statements"]

        query = u"""
        DELETE FROM """ + table + u""" """ + where[u"statements"]

        cursor = self._db.cursor()

        cursor.execute(u"SELECT COUNT(*) FROM " + table + u" " + where[u"statements"], where[u"values"])
        count = cursor.fetchall()[0][0]


        cursor.execute(query, where[u"values"])

        cursor.connection.commit()
        return count

    def insert(self, insert):

        cursor = self._db.cursor()

        cursor.execute(insert[u'statements'], insert[u"values"])

        cursor.connection.commit()
        return cursor.lastrowid
