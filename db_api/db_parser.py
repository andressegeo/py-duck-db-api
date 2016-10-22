# -*- coding: utf-8 -*-

import datetime
import calendar
import re

class DBParser(object):

    def __init__(self):

        self._OPERATORS = {
            u"$eq" : u"=",
            u"$gt" : u">",
            u"$gte" : u">=",
            u"$lt" : u"<",
            u"$lte": u"<=",
            u"$ne": u"!="
        }



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


    def get_formatted_header_for_json(self, headers):
        formated_headers = []

        for header in headers:

            while header.find(u"_") != -1:
                found = header.find(u"_")
                header = header[:found] + header[found+1].upper() + header[found+2:]

            formated_headers.append(header)
        return formated_headers

    def parse_filters(self, filters, operator=u"AND"):
        if type(filters) is not list:
            filters = [filters]
        filters = filters or []
        where = {
            u"statments" : [],
            u"values" : []
        }

        for filter in filters:
            if type(filter) is list:
                pass
            elif type(filter) is dict:
                for key in filter:
                    # If operator
                    if key[0] == u"$" and key in self._OPERATORS:
                        where[u"statments"].append()
                    # If field
                    else:
                        pass



