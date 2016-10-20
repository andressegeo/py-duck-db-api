# -*- coding: utf-8 -*-


class DBRequestParser(object):

    def __init__(
            self,
            db_connection,
            table_columns,
            table_relations
    ):
        self._db_connection = db_connection
        self._table_columns = table_columns
        self._table_relations = table_relations


