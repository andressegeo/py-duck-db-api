# -*- coding: utf-8 -*-


class DBApi(object):
    """
    The class which wraps all the DB Api logic.
    """

    def __init__(
            self,
            db_connection,
            db_parser_def
    ):
        self._db_connection = db_connection
        self._db_parser_def = db_parser_def

    def get_parser(self, table):
        """
        Generate the DB Parser.
        Args:
            table (unicode): The name of the table.

        Returns:
            (DBParser): The parser linked to the table.
        """
        return self._db_parser_def(
            table=table,
            columns=self._db_connection.get_columns(table)
        )

    def list(self, table, filters=None, order_by=None, order=None, limit=20, offset=0):
        """
        Do a list query.
        Args:
            table (unicode): The table name.
            filters (dict): Filters.
            order_by (list): Fields to order by.
            order (list): Order for each field to sort.
            limit (int): Max item fetched.
            offset (int): Page to fetch.

        Returns:
            ((list), boolean): Result & if there is something next.
        """
        # Generate the parser.
        db_parser = self.get_parser(table)
        # Generate the initial state.
        base_state = db_parser.generate_base_state()
        #
        filters = db_parser.parse_match(
            match=filters or {},
            from_state=base_state
        )

        order_by = db_parser.parse_order_by(
            order_by=order_by or [],
            order=order or [],
            from_state=base_state
        )
        items = self._db_connection.select(
            fields=base_state.get(u"fields"),
            table=table,
            joins=base_state.get(u"joins"),
            where=filters,
            formatter=db_parser.rows_to_formated,
            order_by=order_by,
            first=offset,
            nb=limit+1
        )

        has_next = len(items) > limit
        if has_next:
            items.pop(-1)

        return items, has_next

    def create(self, table, item):
        """
        This method create an item in the table.
        Args:
            table (unicode): The table name.
            item (dict): The item representation.

        Returns:
            (int): The ID of the created item.
        """
        # Generate the parser.
        db_parser = self.get_parser(table)
        # Generate insert stmt.
        insert = db_parser.parse_insert(data=item)

        return self._db_connection.insert(
            table=db_parser._table,
            fields=insert[u"fields"],
            positional_values=insert[u'positional_values'],
            values=insert[u"values"]
        )

    def update(self, table, filters, item):
        """
        This method update the table regarding the filters.
        Args:
            table (unicode): The table name.
            filters (dict): Where to update the items.
            item (dict): The updated values.

        Returns:
            (int): The number of updated items.
        """
        db_parser = self.get_parser(table)
        base_state = db_parser.generate_base_state()
        filters = db_parser.parse_match(
            match=filters,
            from_state=base_state,
            use_alias=False
        )

        count = self._db_connection.update(
            table=table,
            update=db_parser.parse_update(
                data=item
            ),
            joins=base_state.get(u"joins"),
            where=filters
        )

        return count

    def delete(self, table, filters):
        """
        This method delete items from the table regarding the filters.
        Args:
            table (unicode): The table name.
            filters (dict): Where to update the items.

        Returns:
            (int): The number of updated items.
        """
        db_parser = self.get_parser(table)
        base_state = db_parser.generate_base_state()
        filters = db_parser.parse_match(
            match=filters,
            from_state=base_state,
            use_alias=False
        )

        if filters.get(u"statements") == u"":
            raise ValueError(u"You need to set a proper filter to delete (safe mode)")

        return self._db_connection.delete(
            table=table,
            joins=base_state.get(u"joins"),
            where=filters,
        )

    def description(self, table):
        """
        Generate a description of the table.
        Args:
            table (unicode): The table name.

        Returns:
            (dict): The description.
        """
        db_parser = self.get_parser(table)
        return db_parser.generate_column_description(
            table=table,
            columns=list(db_parser._base_columns)
        )

