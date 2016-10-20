# -*- coding: utf-8 -*-

import datetime
import calendar


class DBConnection(object):

    def __init__(
            self,
            db_api,
            user,
            password,
            database,
            host=u"127.0.0.1"):

        self._db = db_api.connect(
            host=host,
            user=user,
            passwd=password,
            db=database,
            charset=u"utf8"
        )

        self._database = database

    def get_db_to_python_type(self, db_type):
        if u"int" in db_type:
            return int
        elif u"float" in db_type or u"float" in db_type:
            return float
        elif u"datetime" in db_type:
            return datetime.datetime
        elif u"varchar" in db_type or u"text" in db_type:
            return unicode

    def get_table_columns(self, table):

        cursor = self._db.cursor()

        query = u"DESCRIBE " + table

        cursor.execute(query)
        table_conf_rows = cursor.fetchall()

        fields_conf = {}

        for table_conf_row in table_conf_rows:
            fields_conf[table_conf_row[0]] = {
                u"type": self.get_db_to_python_type(table_conf_row[1]),
                u"required": table_conf_row[2] == u"NO",
                u"key": table_conf_row[3]
            }

        return fields_conf

    def get_foreign(self, table):
        table_relations = self.get_table_relations()
        return [ relation
            for relation in table_relations
            if relation[u"referenced"][u"column"] is not None
            and relation[u"table"] == table
        ]

    def get_formated_referenced_fields(self):
        table_relations = self.get_table_relations()
        return [
            (u"`" + relation[u"referenced"][u"table"] + u"`.`" + relation[u"referenced"][u"column"] + u"`")
            + u" AS `" + relation[u"referenced"][u"table"] + u"." + relation[u"referenced"][u"column"] + u"`"
            for relation in table_relations if relation[u"referenced"][u"column"] is not None
        ]

    def get_formated_fields(self, table):
        table_columns = self.get_table_columns(table)
        return [
            u"`" + table + u"`.`" + column + u"` AS `" + table + u"." + column + u"`" for column in table_columns
            if column not in [foreign[u"column"] for foreign in self.get_foreign(table)]
        ]

    def select(self, table, where=None):
        # Where
        where = where or {
            u"statments": [],
            u"values": []
        }
        # Get foreign table fields
        # Get fields to display
        fields = self.get_formated_fields(table)
        foreign_tables = [ foreign[u"referenced"][u"table"] for foreign in self.get_foreign(table)]
        for foreign_table in foreign_tables:
            fields += self.get_formated_fields(foreign_table)

        # Generate joins
        joins = [
            u" JOIN `" + relation[u"referenced"][u"table"] + u"` ON `" + relation[u"table"]
            + u"`.`" + relation[u"column"] + u"` = `" + relation[u"referenced"][u"table"]
            + u"`.`" + relation[u"referenced"][u"column"] + u"`"
            for relation in self.get_table_relations()
            if relation[u"referenced"][u"column"] is not None
        ]

        # Construct query
        query = u"SELECT " \
            + u", ".join(fields) \
            + u" FROM `" + table + u"`" \
            + u" ".join(joins)

        cursor = self._db.cursor()
        cursor.execute(query)

        return self.rows_to_json(
            table,
            [
            desc[0] for desc in cursor.description
            ], cursor.fetchall())

    def json_put(self, item, path, value):
        tab = path.split(u".")
        if tab[0] not in item and len(tab) > 1:
            item[tab[0]] = {}
        if len(tab) == 1:
            item[tab[0]] = value
        else:
            item[tab[0]] = self.json_put(item[tab[0]], u".".join(tab[1:]), value)
        return item

    def get_formatted_header_for_json(self, headers):
        formated_headers = []

        for header in headers:

            while header.find(u"_") != -1:
                found = header.find(u"_")
                header = header[:found] + header[found+1].upper() + header[found+2:]

            formated_headers.append(header)
        return formated_headers

    def python_type_to_json(self, val):
        if type(val) in [long, float]:
            return float(val)
        elif type(val) is str:
            return val.decode('utf-8')
        elif type(val) is datetime.datetime:
            return calendar.timegm(val.timetuple())
        else:
            return val

    def rows_to_json(self, table, headers, rows):
        headers = self.get_formatted_header_for_json(headers)
        items = []
        for row in rows:
            item = {}
            for index, cell in enumerate(row):
                cell = self.python_type_to_json(cell)
                if table in headers[index]:
                    item[headers[index].replace(table + u".", u"")] = cell
                else:
                    self.json_put(item, headers[index], cell)


            items.append(item)
        return items

    def get_table_relations(self):

        cursor = self._db.cursor()

        query = u"""
        SELECT
        CONSTRAINT_SCHEMA,
        TABLE_SCHEMA,
        TABLE_NAME,
        COLUMN_NAME,
        REFERENCED_TABLE_SCHEMA,
        REFERENCED_TABLE_NAME,
        REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s
        """

        cursor.execute(query, (self._database,))

        constraints = cursor.fetchall()

        return [{
            u"schema": {
                u"constraint": constraint[0],
                u"table": constraint[1]
            },
            u"table": constraint[2],
            u"column": constraint[3],
            u"referenced": {
                u"schema": constraint[4],
                u"table": constraint[5],
                u"column": constraint[6]
            }
        } for constraint in constraints]
