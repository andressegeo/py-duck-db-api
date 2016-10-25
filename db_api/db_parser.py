# -*- coding: utf-8 -*-

import datetime
import calendar
import re


class DBParser(object):

    def __init__(self, table, columns):

        self._OPERATORS = {
            u"$eq": u"=",
            u"$gt": u">",
            u"$gte": u">=",
            u"$lt": u"<",
            u"$lte": u"<=",
            u"$ne": u"!="
        }

        self._RECURSIVE_OPERATORS = {
            u"$and": u"AND",
            u"$or": u"OR"
        }

        self._columns = columns
        self._table = table

    def json_put(self, item, path, value):
        tab = path.split(u".")
        if tab[0] not in item and len(tab) > 1:
            item[tab[0]] = {}
        if len(tab) == 1:
            item[tab[0]] = value
        else:
            item[tab[0]] = self.json_put(item[tab[0]], u".".join(tab[1:]), value)
        return item

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

        def get_table_column_from_header(headers):
            decomposed = [(re.search(u"`(\S+)+`\.*`(\S+)+`", header)).groups() for header in headers]
            return decomposed

        decomposed_headers = get_table_column_from_header(headers)
        items = []
        for row in rows:
            item = {}
            for index, cell in enumerate(row):
                cell = self.python_type_to_json(cell)
                j_field = self.headers_to_json(decomposed_headers[index])
                if table == decomposed_headers[index][0]:
                    item[j_field[1]] = cell
                else:
                    self.json_put(item, j_field[0] + u"." + j_field[1], cell)

            items.append(item)
        return items

    def headers_to_json(self, headers):
        formated_headers = []
        for header in headers:
            while header.find(u"_") != -1:
                found = header.find(u"_")
                header = header[:found] + header[found+1].upper() + header[found+2:]

            formated_headers.append(header)

        return formated_headers

    def json_to_header(self, json_field, use_referenced=False):
        def insert_underscore(input):
            indexes = [i for i, ltr in enumerate(input) if ltr.isupper()]
            indexes.reverse()
            for index in indexes:
                input = input[0:index] + u"_" + input[index:].lower()
            return input

        # Not a referenced field
        if u"." not in json_field:
            return u"`" + insert_underscore(self._table) + u"`.`" + insert_underscore(json_field) + u"`"
        # Reference field
        else:
            table, field = tuple(json_field.split(u"."))

            if use_referenced:
                for ref in self.get_columns_with_reference():
                    if (ref[u"referenced_table_name"] == table
                        and ref[u"referenced_column_name"] == field):
                        table, field = ref[u"table_name"], ref[u"column_name"]
                        break

            return u"`" + insert_underscore(table) + u"`.`" + insert_underscore(field) + u"`"

    def get_columns_with_reference(self):
        return [column for column in self._columns
            if u"referenced_table_name" in column]

    def is_field(self, key):
        db_field = self.json_to_header(key)

        valid_columns = []

        for column in self._columns:
            valid_columns.append(u"`" + column[u"table_name"]
                + u"`.`" + column[u"column_name"] + u"`"
            )
            if u"referenced_table_name" in column:
                valid_columns.append(u"`" + column[u"referenced_table_name"]
                    + u"`.`" + column[u"referenced_column_name"] + u"`"
                )

        return db_field in valid_columns

    def parse_filters(self, filters, operator=u"AND", parent=None):
        filters = filters or {}

        if type(filters) is not list:
            filters = [filters]

        where = {
            u"statements": [],
            u"values": []
        }

        for filter in filters:
            for key in filter:

                # If key is an operator
                if self.is_field(key):

                    db_field = self.json_to_header(key)
                    value = self.get_wrapped_values([db_field], [filter[key]])
                    if type(filter[key]) in [unicode, str, int, float]:

                        where[u"statements"].append(db_field + u" = " + value)
                        where[u"values"].append(filter[key])

                    elif type(filter[key]) is dict:

                        ret = self.parse_filters(filter[key], parent=key)
                        where[u"statements"].append(ret[u"statements"])
                        where[u"values"] += ret[u"values"]

                elif key in self._OPERATORS and parent is not None:

                    db_field = self.json_to_header(parent)
                    value = self.get_wrapped_values([db_field], [filter[key]])
                    where[u"statements"].append(db_field + u" " + self._OPERATORS[key] + u" " + value)
                    where[u"values"].append(filter[key])

                elif key in self._RECURSIVE_OPERATORS:

                    ret = self.parse_filters(filter[key], self._RECURSIVE_OPERATORS[key], parent=key)
                    where[u"statements"].append(u"(" + ret[u"statements"] + u")")
                    where[u"values"] += ret[u"values"]

        where[u"statements"] = (u" " + operator + u" ").join(where[u"statements"])

        return where

    def parse_update(self, data):
        update = {
            u"statements": [],
            u"values": []
        }



        if u"$set" in data:
            data[u"$set"] = self.to_one_level_json(data[u"$set"])
            db_fields, values = zip(*[
                (
                    self.json_to_header(field, use_referenced=True),
                    data[u"$set"][field]
                ) for field in data[u"$set"]]
            )

            for index, db_field in enumerate(db_fields):
                if self._table in db_field:
                    wrapped = self.get_wrapped_values([db_field], [values[index]])
                    update[u"statements"] += [(db_field + u" = " + wrapped)]
                    update[u"values"].append(values[index])



        update[u"statements"] = u", ".join(update[u"statements"])

        if update[u"statements"] != u"":
            update[u"statements"] = u"SET " + update[u"statements"]

        return update

    def to_one_level_json(self, obj, parent=u""):
        output = {}
        if parent != u"":
            parent += u"."

        for key in obj:
            if type(obj[key]) is dict:
                output.update(self.to_one_level_json(obj[key], parent=key))
            else:
                output[(parent + key)] = obj[key]

        return output

    def get_wrapped_values(self, headers, values):

        output = []

        for index, header in enumerate(headers):
            for column in self._columns:
                if (u"`" + column[u"table_name"] + u"`.`" + column[u"column_name"] + u"`") == header:

                    if u"datetime" in column[u"type"] and type(values[index]) in [int, float]:
                        output.append(u"FROM_UNIXTIME(%s)")
                    else:
                        output.append(u"%s")

                    break
        return u", ".join(output)

    def parse_insert(self, data):
        insert = {
            u"statements": u"INSERT INTO `" + self._table + u"`(#fields) VALUES(#values)",
            u"values": []
        }

        one_level_data = self.to_one_level_json(data)

        db_fields, insert[u"values"] = zip(*[
            (self.json_to_header(field, use_referenced=True), one_level_data[field])
            for field in one_level_data
            if self._table in self.json_to_header(field, use_referenced=True)
        ])


        insert[u"statements"] = insert[u"statements"].replace(u"#fields", u", ".join(list(db_fields)))
        insert[u"statements"] = insert[u"statements"].replace(u"#values", self.get_wrapped_values(db_fields, insert[u"values"]))


        return insert
