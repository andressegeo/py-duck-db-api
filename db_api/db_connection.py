# -*- coding: utf-8 -*-

import json
import logging
import csv

class DBConnection(object):

    def __init__(
            self,
            db_api_def,
            user,
            password,
            database,
            host=u"127.0.0.1"):

        self._db_api_def = db_api_def
        self._host = host
        self._user = user
        self._password = password
        self._database = database
        self._connect()

    def _connect(self):
        self._db = self._db_api_def.connect(
            host=self._host,
            user=self._user,
            passwd=self._password,
            db=self._database,
            charset=u"utf8"
        )

    def _do_reconnect_if_needed(self, e):
        if e[0] == 2006:
            logging.info(u"Connection lost. Reconnecting ... {}".format(e))
            self._connect()
            logging.info(u"Connection recovered")
        else:
            logging.warning(u"Incident : {}".format(e))
            raise e

    def _execute(self, query, values=None, do_commit=True):
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
        try:
            cursor.execute(query, values)
        except self._db_api_def.OperationalError as e:
            self._do_reconnect_if_needed(e)
            cursor.execute(query, values)

        if do_commit:
            cursor.connection.commit()
        return cursor.fetchall(), cursor.description

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
        fetched, _ = self._execute(query, values=(self._database, table))
        referenced = [
            {
                u"table_name": constraint[0],
                u"column_name": constraint[1],
                u"referenced_table_name": constraint[2],
                u"referenced_column_name": constraint[3],
                u"referenced_alias": constraint[1]
            }
            for constraint in fetched
            ]
        for ref in referenced:
            referenced += self.get_referenced(ref[u"referenced_table_name"])
        return referenced

    def get_columns(self, table, alias=None):
        referenced = self.get_referenced(table)
        query = u"""
                DESCRIBE
                """ + table + """"""
        fetched, _ = self._execute(query)
        columns = []
        # For each row
        for row in fetched:
            column = {
                u"table_name": table,
                u"column_name": row[0],
                u"type": row[1],
                u"null": row[2] == u"YES",
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

    def get_columns(self, table, alias=None):

        referenced = self.get_referenced(table)

        query = u"""
        DESCRIBE
        """ + table + """"""

        fetched, _ = self._execute(query)

        columns = []
        # For each row
        for row in fetched:
            column = {
                u"table_name": table,
                u"column_name": row[0],
                u"type": row[1],
                u"null": row[2] == u"YES",
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

    def _base_join(self, joins):
        return [
            (
                u"""
                JOIN `{}`
                AS `{}`
                ON `{}`.`{}` = `{}`.`{}`
                """
            ).format(
                ref.get(u"to_table"),
                u".".join(ref.get(u"to_path")),
                u".".join(ref.get(u"from_path")),
                ref.get(u"from_column"),
                u".".join(ref.get(u"to_path")),
                ref.get(u"to_column")
            )
            for ref in joins
        ]

    def _base_query(self, fields, table, joins):
        headers = [
            u".".join(field.get(u"path") + [field.get(u"name")])
            for field in fields
            ]

        fields = [
            u"`{}`.`{}` AS `{}`".format(u".".join(field.get(u"path")), field.get(u"name"),
                                        u".".join(field.get(u"path") + [field.get(u"name")]))
            for field in fields
        ]

        joins = self._base_join(joins)

        query = u"SELECT " + u", ".join(fields) + u" FROM `" + table + u"` " + u" ".join(joins)

        return headers, query

    def update(self, table, joins, update, where):

        table = table or self._table

        count = 0
        if where[u"statements"] != u"":
            where[u"statements"] = u" WHERE " + where[u"statements"]

        if update[u"statements"] != u"":
            update[u"statements"] = u" SET " + u", ".join(update[u'statements'])

        joins = u" ".join(self._base_join(joins))

        query = u"""UPDATE """ + table + u" " + joins + u""" """ + update[u"statements"] + where[u"statements"]

        fetched, _ = self._execute(
            u"SELECT COUNT(*) FROM " + table + u" " + joins + u" " + where[u"statements"],
            where[u"values"]
        )
        count = fetched[0][0]
        # self._execute(query, update[u"values"] + where[u"values"])
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

        joins = u" ".join(self._base_join(joins))

        end_query = u" " + joins + u" " + where[u"statements"]

        count_query = u"SELECT COUNT(*) FROM " + table + end_query

        # Determine how many lines are going to be deleted
        fetched, _ = self._execute(
            query=count_query,
            values=where[u"values"]
        )
        count = fetched[0][0]

        query = u"""DELETE """ + table + u" FROM " + table + end_query

        # Execute the final query
        self._execute(
            query=query,
            values=where[u'values']
        )

        return count

    def select(
            self,
            fields,
            table,
            joins,
            where=None,
            formatter=None,
            order_by=None,
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

        if order_by is not None and order_by[u"statements"] != u"":
            query += u" ORDER BY " + order_by[u"statements"]

        query += u" LIMIT %s OFFSET %s"

        fetched, description = self._execute(query, (where[u'values'] + [int(nb), int(first)]))
        # If formater in parameter
        if formatter is not None:
            return formatter(
                headers,
                fetched,
                fields,
                ignore_prefix=True
            )

        return [i[0] for i in description], fetched

    def aggregate(self, table, base_state, stages=None, formater=None):
        stages = stages or []

        headers, query = self._base_query(
            fields=base_state.get(u"fields"),
            table=table,
            joins=base_state.get(u"joins")
        )
        # Base query
        query = u"SELECT * FROM (" + query + u") AS s_0"

        values = []
        last_state = None
        # For each stage
        for index, stage in enumerate(stages):

            parsed = stage.get(u"parsed")
            stage_type = stage.get(u"type")
            if stage_type == u"match":

                query = u"SELECT * FROM ( {} ) AS s_{}".format(
                    query, 
                    index+1,
                )

                if len(parsed.get(u"values")) > 0:
                    query += u" WHERE {}".format(
                        parsed.get(u"statements")
                    )
                    values += parsed.get(u"values", [])

            elif stage_type == u"project":
                query = u"SELECT {} FROM ( {} ) AS s_{}".format(
                    parsed.get(u"statements"),
                    query,
                    index+1,
                )
                values = parsed.get(u"values") + values
                last_state = parsed.get(u"state")
            elif stage_type == u"group":
                query = u"SELECT {} FROM ( {} ) AS s_{}".format(
                    u", ".join(parsed.get(u"fields", [])),
                    query,
                    index + 1
                )
                if len(parsed.get(u"group_by", [])) > 0:
                    query += u" GROUP BY {}".format(u", ".join(parsed.get(u"group_by")))

                values = parsed.get(u"values") + values
                last_state = parsed.get(u"state")
            elif stage_type == u"orderby":
                query = u"SELECT * FROM ( {} ) AS s_{}".format(
                    query,
                    index + 1
                )
                if parsed.get(u"statements", u"") != u"":
                    query += u" ORDER BY {}".format(parsed.get(u"statements"))

        last_state = last_state or base_state
        fetched, description = self._execute(query, values)

        headers = [
            u".".join(field.get(u"path") + [field.get(u"name")])
            for field in last_state.get(u"fields", [])
        ]
        # If formater in parameter
        if formater is not None:
            return formater(
                headers,
                fetched,
                last_state.get(u"fields"),
                ignore_prefix=last_state is not None
            )

        return [i[0] for i in description], fetched

    def insert(self, table, fields, positional_values, values):
        cursor = self._db.cursor()

        query = u"INSERT INTO {}({}) VALUES ({})".format(table, u", ".join(fields), u", ".join(positional_values))
        try:
            cursor.execute(query, values)
        except self._db_api_def.OperationalError as e:
            self._do_reconnect_if_needed(e)
            cursor.execute(query, values)

        cursor.connection.commit()
        return cursor.lastrowid

    def export(self, headers, rows):
        pass

