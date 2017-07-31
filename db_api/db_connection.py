# -*- coding: utf-8 -*-


class DBConnection(object):

    def __init__(
            self,
            db_api_def,
            user,
            password,
            database,
            unix_socket,
            host
        ):

        self._db_api_def = db_api_def
        self._host = host
        self._user = user
        self._password = password
        self._database = database
        self._unix_socket = unix_socket
        self._referenced_cache = {}
        
    def connect(self):
        params = {
            u"user": self._user,
            u"passwd": self._password,
            u"db": self._database,
            u"charset": u"utf8"
        }
        
        if self._host:
            params[u"host"] = self._host
        else:
            params[u"unix_socket"] = self._unix_socket
        return self._db_api_def.connect(**params)

    def _execute(self, query, values=None, custom_cursor=None):
        """
        :type query: string
        :param query: The SQL query to execute
        :type values: List
        :param values: The values to sanitize & pass to the query to replace the "%s" values.
        :return:
        """

        cursor = custom_cursor

        if custom_cursor is None:
            # Create connection
            db = self.connect()
            cursor = db.cursor()
            # Execute query
            cursor.execute(query, values)
            ret = cursor.fetchall(), cursor.description
            # Commit & close
            cursor.connection.commit()
            cursor.close()
            db.close()
        else:
            # Just execute, opening & close handled somewhere else
            cursor.execute(query, values)
            ret = cursor.fetchall(), cursor.description

        return ret

    def get_referenced(self, table):
        if table in self._referenced_cache:
            return self._referenced_cache[table]

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
                u"referenced_alias": constraint[0] + u"_" + constraint[1]
            }
            for constraint in fetched
            ]
        for ref in referenced:
            referenced += self.get_referenced(ref[u"referenced_table_name"])

        self._referenced_cache[table] = referenced

        return referenced

    def get_columns(self, table, alias=None):
        referenced = self.get_referenced(table)
        query = u"""
                DESCRIBE
                """ + table + u""""""
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

        headers, query = self._base_query(fields, table, joins)

        if where is not None and where[u"statements"] != u"":
            query = u"SELECT * FROM (" + query + u") AS s_0 WHERE " + where[u"statements"]

        if order_by is not None and order_by[u"statements"] != u"":
            query += u" ORDER BY " + order_by[u"statements"]

        values = where[u'values'] + [nb+1, first]
        query += u" LIMIT %s OFFSET %s"
        fetched, description = self._execute(query, values)

        has_next = False
        if len(fetched) > nb:
            has_next = True
            fetched = fetched[:-1]

        # If formatter in parameter
        if formatter is not None:
            return formatter(
                headers,
                fetched,
                fields,
                ignore_prefix=True
            ), has_next

        return ([i[0] for i in description], fetched), has_next

    def aggregate(self, table, base_state, stages=None, formatter=None, skip=0, limit=100):
        """
        Generate the Aggregation query.
        Args:
            table (unicode): The table to query.
            base_state (dict): The state we are starting from.
            stages (list): The differents stage to interpret from the pipeline.
            formatter (funct): A method to format output.
            skip (int): how many lines we skip.
            limit (int): Max line count we want.

        Returns:
            (list, list): Columns & fetched items.
        """
        stages = stages or []
        ignore_prefix = True
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
        index = 0
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
                ignore_prefix = False
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
                ignore_prefix = False
                values = parsed.get(u"values") + values
                last_state = parsed.get(u"state")
            elif stage_type == u"orderby":
                query = u"SELECT * FROM ( {} ) AS s_{}".format(
                    query,
                    index + 1
                )
                if parsed.get(u"statements", u"") != u"":
                    query += u" ORDER BY {}".format(parsed.get(u"statements"))

        query = u"SELECT * FROM ({}) AS s_{} LIMIT {} OFFSET {}".format(query, index+1, limit+1, skip)

        last_state = last_state or base_state
        fetched, description = self._execute(query, values)

        has_next = False
        if len(fetched) > limit:
            has_next = True
            fetched = fetched[:-1]

        headers = [
            u".".join(field.get(u"path") + [field.get(u"name")])
            for field in last_state.get(u"fields", [])
        ]

        # If formatter in parameter
        if formatter is not None:
            return formatter(
                headers,
                fetched,
                last_state.get(u"fields"),
                ignore_prefix=ignore_prefix
            ), has_next

        return ([i[0] for i in description], fetched), has_next

    def insert(self, table, fields, positional_values, values):
        db = self.connect()
        cursor = db.cursor()

        query = u"INSERT INTO {}({}) VALUES ({})".format(table, u", ".join(fields), u", ".join(positional_values))
        cursor.execute(query, values)

        cursor.connection.commit()
        last_row_id = cursor.lastrowid

        cursor.close()
        db.close()

        return last_row_id

    def export(self, headers, rows):
        pass

