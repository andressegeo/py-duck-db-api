# -*- coding: utf-8 -*-

import csv
import StringIO

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

    def export(self, table, filters=None, pipeline=None):
        """

        Args:
            table:
            filters:
            pipeline:
            offset:
            limit:

        Returns:

        """
        db_parser = self.get_parser(table)
        base_state = db_parser.generate_base_state()

        if not filters and not pipeline:
            filters = {}

        if pipeline is not None:
            stages = self._pipeline_to_stages(
                db_parser=db_parser,
                pipeline=pipeline
            )

            headers, rows = self._db_connection.aggregate(
                table,
                base_state=base_state,
                stages=stages
            )

        elif filters is not None:
            filters = db_parser.parse_match(
                match=filters,
                from_state=base_state
            )
            headers, rows = self._db_connection.select(
                fields=base_state.get(u"fields"),
                table=table,
                joins=base_state.get(u"joins"),
                where=filters,
                first=0,
                nb=None
            )
        return headers, rows

    @staticmethod
    def export_to_csv(headers, rows, options):

        output = StringIO.StringIO()
        encoding = options.get(u"encoding", u"utf-8")
        # Open parsers
        writer = csv.writer(
            output,
            delimiter=options.get(u"delimiter", u"\t").encode(encoding)
        )

        for row in [headers] + list(rows):
            line = [unicode(cell).encode(encoding) for cell in row]
            writer.writerow(line)

        return output

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

    def aggregate(self, table, pipeline, skip=0, limit=100):
        """
        Do an aggregate request.
        Args:
            table (unicode): The table name.
            pipeline (list): A list of dict representing the pipeline.
            skip (int): Number of row to pass.
            limit (int): Max rows count returned.

        Returns:
            (list): A list of item resulting the request.
        """
        db_parser = self.get_parser(table)
        db_parser.generate_base_state()

        stages = self._pipeline_to_stages(
            db_parser=db_parser,
            pipeline=pipeline
        )

        items, has_next = self._db_connection.aggregate(
            table,
            db_parser.generate_base_state(),
            stages,
            skip=skip,
            limit=limit,
            formatter=db_parser.rows_to_formated
        )

        return items, has_next

    def _pipeline_to_stages(self, db_parser, pipeline):
        base_state = db_parser.generate_base_state()
        stages = []
        custom_state = None

        for stage in pipeline:
            if u"$match" in stage:
                ret = db_parser.parse_match(
                    match=stage.get(u"$match", {}),
                    from_state=custom_state or base_state
                )
                stages.append(
                    {
                        u"type": u"match",
                        u"parsed": ret
                    }
                )
            elif u"$project" in stage:
                ret = db_parser.parse_project(
                    stage.get(u"$project"),
                    from_state=custom_state or base_state
                )
                stages.append(
                    {
                        u"type": u"project",
                        u"parsed": ret
                    }
                )
                # Project alter the state. Use custom one
                custom_state = ret[u'state']
            elif u"$group" in stage:
                ret = db_parser.parse_group(
                    group=stage.get(u"$group"),
                    from_state=custom_state or base_state
                )
                stages.append(
                    {
                        u"type": u"group",
                        u"parsed": ret
                    }
                )
                # Group alter the state. Use custom one
                custom_state = ret[u'state']
            elif u"$orderby" in stage:
                ret = db_parser.parse_order_by(
                    order_by=stage.get(u"$orderby"),
                    from_state=custom_state or base_state
                )
                stages.append(
                    {
                        u"type": u"orderby",
                        u"parsed": ret
                    }
                )

        return stages