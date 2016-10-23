# -*- coding: utf-8 -*-

import datetime
import calendar
import re


class DBParser(object):

    def __init__(self, table, headers, referenced):

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

        self._headers = headers
        self._referenced = referenced
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

    def get_table_column_from_header(self, headers):
        decomposed = [(re.search(u"`(\S+)+`\.*`(\S+)+`", header)).groups() for header in headers]


        return decomposed

    def rows_to_json(self, table, headers, rows):

        decomposed_headers = self.get_table_column_from_header(headers)
        items = []
        for row in rows:
            item = {}
            for index, cell in enumerate(row):
                cell = self.python_type_to_json(cell)
                if table == decomposed_headers[index][0]:
                    item[decomposed_headers[index][1]] = cell
                else:
                    self.json_put(item, decomposed_headers[index][0] + u"." + decomposed_headers[index][1], cell)

            items.append(item)
        return items

    def formatted_header_to_json(self, headers):
        formated_headers = []
        for header in headers:
            while header.find(u"_") != -1:
                found = header.find(u"_")
                header = header[:found] + header[found+1].upper() + header[found+2:]

            formated_headers.append(header)
        return formated_headers

    def json_to_formatted_header(self, json_field):
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
            return u"`" + insert_underscore(table) + u"`.`" + insert_underscore(field) + u"`"

    def is_field(self, key):
        db_field = self.json_to_formatted_header(key)
        if db_field in self._headers:
            return True
        return False

    def parse_filters(self, filters, operator=u"AND", parent=None):

        filters = filters or []

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

                    db_field = self.json_to_formatted_header(key)

                    if type(filter[key]) in [unicode, str, int, float]:

                        where[u"statements"].append(db_field + u" = %s")
                        where[u"values"].append(filter[key])

                    elif type(filter[key]) is dict:

                        ret = self.parse_filters(filter[key], parent=key)
                        where[u"statements"].append(ret[u"statements"])
                        where[u"values"] += ret[u"values"]

                elif key in self._OPERATORS and parent is not None:

                    db_field = self.json_to_formatted_header(parent)
                    where[u"statements"].append(db_field + u" " + self._OPERATORS[key] + u" %s")
                    where[u"values"].append(filter[key])

                elif key in self._RECURSIVE_OPERATORS:

                    ret = self.parse_filters(filter[key], self._RECURSIVE_OPERATORS[key], parent=key)
                    where[u"statements"].append(u"(" + ret[u"statements"] + u")")
                    where[u"values"] += ret[u"values"]

        where[u"statements"] = (u" " + operator + u" ").join(where[u"statements"])
        print(where)
        return where



