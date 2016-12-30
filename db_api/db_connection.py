# -*- coding: utf-8 -*-

import json
import logging


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

    @staticmethod
    def _reconnect_on_exception(exception_to_handle, reconnect_method):
        def decorator(function):
            def wrapper(*args, **kwargs):
                retry = 1
                while retry >= 0:
                    try:
                        return function(*args, **kwargs)
                    except exception_to_handle as e:
                        if retry == 0:
                            logging.error(str(e))
                            raise e
                        else:
                            logging.warning(str(e))
                        # Reconnect
                        retry -= 1
                        reconnect_method()


            return wrapper
        return decorator

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

        @self._reconnect_on_exception(
            self._db_api_def.OperationalError,
            self._connect
        )
        def wrapper():
            cursor = self._db.cursor()
            cursor.execute(query, values)

            if do_commit:
                cursor.connection.commit()
            return cursor.fetchall()

        return wrapper()

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

        fetched = self._execute(query, values=(self._database, table))

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

        fetched = self._execute(query)

        columns = []
        # For each row
        for row in fetched:
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

    @staticmethod
    def _base_query(fields, table, joins):
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

        fetched = self._execute(
            u"SELECT COUNT(*) FROM " + table + u" " + joins + u" " + where[u"statements"],
            where[u"values"]
        )

        count = fetched[0][0]

        self._execute(query, update[u"values"] + where[u"values"])

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
        fetched = self._execute(
            query=u"SELECT COUNT(*) FROM " + table + u" " + joins + u" " + where[u"statements"], 
            values=where[u"values"]
        )
        count = fetched[0][0]

        # Execute the final query
        self._execute(
            query=u"""DELETE """ + table + u" FROM " + table + u" " + joins + u""" """ + where[u"statements"],
            values=where[u'values']
        )

        return count

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

        fetched = self._execute(query, (where[u'values'] + [int(nb), int(first)]))

        # If formater in parameter
        if formater is not None:
            return formater(
                headers,
                fetched,
                fields
            )

        return headers, fetched

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

        last_state = last_state or base_state
        fetched = self._execute(query, values)

        # If formater in parameter
        if formater is not None:
            return formater(
                headers,
                fetched,
                last_state.get(u"fields")
            )

        return headers, fetched

    def insert(self, table, fields, positional_values, values):
        @self._reconnect_on_exception(
            self._db_api_def.OperationalError,
            self._connect
        )
        def wrapper():

            cursor = self._db.cursor()

            query = u"INSERT INTO {}({}) VALUES ({})".format(table, u", ".join(fields), u", ".join(positional_values))
            cursor.execute(query, values)
            cursor.connection.commit()
            return cursor.lastrowid

        return wrapper()

