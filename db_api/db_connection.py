# -*- coding: utf-8 -*-

import datetime
import calendar
import json

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

        referenced = [
            {
                u"table_name": constraint[0],
                u"column_name": constraint[1],
                u"referenced_table_name": constraint[2],
                u"referenced_column_name": constraint[3],
                u"referenced_alias": constraint[1]
            }
            for constraint in cursor.fetchall()
            ]

        for ref in referenced:
            referenced += self.get_referenced(ref[u"referenced_table_name"])

        return referenced

    def get_columns(self, table, alias=None):

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
                u"type": row[1],
                u"key": row[3].lower(),
                u"extra": row[5].lower()
            }
            if alias is not None:
                column[u"alias"] = alias
            else:
                column[u"alias"] = table

            # If reference found, add it
            for ref in referenced:
                if (
                    ref.get(u"table_name") == column.get(u"table_name")
                    and ref.get(u"column_name") == column.get(u"column_name")
                ):
                    column.update(ref)
                    columns += self.get_columns(
                        ref.get(u"referenced_table_name"),
                        alias=ref.get(u"column_name")
                    )
                    break
            columns.append(column)
        return columns

    def _base_query(self, fields, table, joins):
        

        headers = [
            field.get(u"db") +
            u" AS `" +
            field.get(u"alias") + u"`"
            for field in fields
        ]

        joins = [
            (
                u"JOIN `" + ref.get(u"referenced_table_name")
                + u"` AS `" + ref.get(u"referenced_alias")
                + u"` ON `"
                + ref.get(u"alias")
                + u"`.`" + ref.get(u"column_name") + u"` = `"
                + ref.get(u"referenced_alias")
                + u"`.`" + ref.get(u"referenced_column_name") + u"`"
            )
            for ref in joins
        ]
        query = u"SELECT " + u", ".join(headers) + u" FROM " + table + u" " + (u" ".join(joins))
        
        return headers, query

    def select(
            self,
            fields,
            table,
            joins,
            where=None,
            formater=None,
            first=0,
            nb=100
    ):
        if first is None:
            first = 0
        if nb is None:
            nb = 100

        headers, query = self._base_query(fields, table, joins)

        if where is not None and where[u"statements"] != u"":
            query = u"SELECT * FROM (" + query + u") AS s_0 WHERE " + where[u"statements"]

        query += u" LIMIT %s OFFSET %s"

        cursor = self._db.cursor()
        cursor.execute(query, (where[u'values'] + [int(nb), int(first)]))

        # If formater in parameter
        if formater is not None:
            return formater(
                headers,
                cursor.fetchall(),
                fields
            )

        return headers, cursor.fetchall()

    def update(self, table, joins, update, where):

        if where[u"statements"] != u"":
            where[u"statements"] = u" WHERE " + where[u"statements"]

        joins = u" ".join([
            (
                u"JOIN `" + ref.get(u"referenced_table_name")
                + u"` AS `" + ref.get(u"referenced_alias")
                + u"` ON `"
                + ref.get(u"alias")
                + u"`.`" + ref.get(u"column_name") + u"` = `"
                + ref.get(u"referenced_alias")
                + u"`.`" + ref.get(u"referenced_column_name") + u"`"
            )
            for ref in joins
        ])

        query = u"""UPDATE """ + table + u" " + joins + u""" """ + update[u"statements"] + where[u"statements"]

        cursor = self._db.cursor()

        cursor.execute(u"SELECT COUNT(*) FROM " + table + u" " + joins + u" " + where[u"statements"], where[u"values"])
        count = cursor.fetchall()[0][0]

        cursor.execute(query, update[u"values"] + where[u"values"])
        cursor.connection.commit()

        return count

    def delete(self, table, joins, where):
        """
        :param table: The table concerned by this method.
        :param joins: The list of table to join with SQL (SQL agnostic parameter).
        :param where: The where clause to apply to the Delete
        :return: The deleted lines count
        """
        if where[u"statements"] != u"":
            where[u"statements"] = u"WHERE " + where[u"statements"]

        joins = u" ".join([
            (
                u"JOIN `" + ref.get(u"referenced_table_name")
                + u"` AS `" + ref.get(u"referenced_alias")
                + u"` ON `"
                + ref.get(u"alias")
                + u"`.`" + ref.get(u"column_name") + u"` = `"
                + ref.get(u"referenced_alias")
                + u"`.`" + ref.get(u"referenced_column_name") + u"`"
            )
            for ref in joins
        ])

        # Determine how many lines are going to be deleted
        ret = self._execute(
            query=u"SELECT COUNT(*) FROM " + table + u" " + joins + u" " + where[u"statements"], 
            values=where[u"values"]
        )
        count = ret[0][0]

        # Execute the final query
        self._execute(
            query=u"""DELETE """ + table + u" FROM " + table + u" " + joins + u""" """ + where[u"statements"],
            values=where[u'values']
        )

        return count

    def _execute(self, query, values, do_commit=True):
        """
        :type query: string
        :param query: The SQL query to execute
        :type values: List
        :param values: The values to sanitize & pass to the query to replace the "%s" values.
        :type do_commit: Bool
        :param do_commit: If the value has to be saved immediately, or can allow a rollback.
        :return:
        """
        cursor = self._db.cursor()
        cursor.execute(query, values)
        if do_commit:
            cursor.connection.commit()
        return cursor.fetchall()

    def aggregate(self, base_dependencies, formater, stages=None):
        stages = stages or []
        fields, table, joins, _ = base_dependencies

        headers, query = self._base_query(fields, table, joins, use_alias=True)

        values = []
        # For each stage
        for index, (stage, value) in enumerate(stages):
            if stage == u"$match":
                query = "SELECT * FROM ( {} ) AS s_{}".format(
                    query, 
                    index, 
                )
                if len(value.get(u"statements")) > 0:
                    query += " WHERE {}".format(
                        value.get(u'statements')
                    )
                    values += value.get(u"values", [])
            elif stage == u"$project":
                fields = value.get(u"dependencies")[0]
                headers = [item.get(u"alias") for item in fields]
                query = "SELECT {} FROM ( {} ) AS s_{}".format(
                    value.get(u"statements"),
                    query,
                    index,
                )
                values = value.get(u"values") + values

        values = self._execute(query, values)

        # If formater in parameter
        if formater is not None:
            return formater(
                headers,
                values,
                fields
            )

        return headers, values

    def insert(self, insert):

        cursor = self._db.cursor()

        cursor.execute(insert[u'statements'], insert[u"values"])

        cursor.connection.commit()
        return cursor.lastrowid
