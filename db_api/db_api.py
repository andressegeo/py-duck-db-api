# -*- coding: utf-8 -*-


class DBApi(object):

    def __init__(
            self,
            db_connection,
            db_parser_def
    ):
        self._db_connection = db_connection
        self._db_parser_def = db_parser_def

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
        db_parser = self._db_parser_def(
            table=table,
            columns=self._db_connection.get_columns(table)
        )
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


    def create(self, item):
        pass

    def update(self, filters, item):
        pass

    def delete(self, filters):
        pass