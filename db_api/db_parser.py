# -*- coding: utf-8 -*-

import datetime
import calendar
import re
import json


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

        self._STAGES_BINDINGS = {
            u"$match": self.match,
            u"$group": self.group
        }
        
        self._RECURSIVE_OPERATORS = {
            u"$and": u"AND",
            u"$or": u"OR"
        }

        self._columns = columns
        self._table = table

    def generate_column_description(self, table, columns):

        types_desc = {
            u"number": [u"int", u"float"],
            u"text": [u"varchar", u"text"],
            u"timestamp": [u"date"]
        }

        ret = []

        for col in [col for col in columns if col[u"table_name"] == table]:

            matching_type = None

            for key in types_desc:
                for db_type in types_desc[key]:
                    if db_type in col.get(u'type'):
                        matching_type = key
                        break

            if matching_type is None:
                raise ValueError(u"No matching types")

            col_desc = {
                u"name": self.headers_to_json([col.get(u'column_name')])[0],
                u"type": matching_type
            }

            if col.get(u'key', u"") != u"":
                col_desc['key'] = col.get(u'key')

            if col.get(u'extra', u"") != u"":
                col_desc['extra'] = col.get(u'extra')

            if u"referenced_table_name" in col:
                col_desc[u'deduceFrom'] = {
                    u"source": col.get(u"referenced_table_name"),
                    u"column": self.headers_to_json([col.get(u"referenced_column_name")])[0]
                }

            ret += [col_desc]

        return ret

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

    def rows_to_formated(self, headers, rows, fields):

        items = []
        for row in rows:
            item = {}
            for index, cell in enumerate(row):
                item = self.json_put(item, fields[index][u'formated'], self.python_type_to_json(cell))

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

    def formated_to_header(self, json_field, use_referenced=False, use_alias=False, dependencies=None):
        if dependencies is None:
            dependencies = self.generate_dependencies()

        db_field = None

        for field in dependencies[0]:
            if field.get(u"formated") == json_field:
                if use_alias:
                    db_field = field.get(u"alias")
                else:
                    db_field = field.get(u"db")
                break

        if use_referenced:
            for ref in dependencies[2]:
                if db_field == (u"`" + ref[u"referenced_alias"] + u"`.`" + ref[u"referenced_column_name"] + u"`"):
                    if use_alias:
                        db_field = (u"`" + ref[u"table_name"] + u"." + ref[u"column_name"] + u"`")
                    else:
                        db_field = (u"`" + ref[u"table_name"] + u"`.`" + ref[u"column_name"] + u"`")
                    break
        return db_field

    def get_columns_with_reference(self):
        return [column for column in self._columns
            if u"referenced_table_name" in column]

    def is_field(self, key):

        db_field = self.formated_to_header(key)
        valid_columns = []

        for column in self._columns:
            valid_columns.append(u"`" + column[u"alias"]
                + u"`.`" + column[u"column_name"] + u"`"
            )
            if u"referenced_table_name" in column:
                valid_columns.append(u"`" + column[u"alias"]
                    + u"`.`" + column[u"referenced_column_name"] + u"`"
                )

        return db_field in valid_columns

    def parse_project(self, project, dependencies=None):
        ret = {
            u"statements": [],
            u"values": [],
            u"dependencies": ([], u"", [], [])  # Dependencies for potential next stage
        }

        for key in project:
            if project[key] == 1:
                db_field = self.formated_to_header(key, use_alias=True, dependencies=dependencies)
                ret[u'statements'].append(db_field)
                ret[u'dependencies'][0].append({
                    u"alias": db_field[1:-1],
                    u"db_field": db_field[1:-1]
                })
            elif type(project[key]) is unicode and u"$" in project[key]:
                db_field = self.formated_to_header(project[key][1:], use_alias=True, dependencies=dependencies)
                ret[u'statements'].append(u"{} AS %s".format(db_field))
                ret[u'values'].append(key)
                ret[u'dependencies'][0].append({
                    u"alias": key,
                    u"db_field": key
                })
            else:
                pass

        ret[u'statements'] = u", ".join(ret[u'statements'])
        return ret


    def parse_filters(self, filters, operator=u"AND", parent=None, use_alias=False, dependencies=None):
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
                    db_field = self.formated_to_header(key, use_alias=use_alias, dependencies=dependencies)
                    value = self.get_wrapped_values([db_field], [filter[key]], use_alias=use_alias)
                    if type(filter[key]) in [unicode, str, int, float]:
                        where[u"statements"].append(db_field + u" = " + value)
                        where[u"values"].append(filter[key])

                    elif type(filter[key]) is dict:

                        ret = self.parse_filters(filter[key], parent=key, use_alias=use_alias, dependencies=dependencies)
                        where[u"statements"].append(ret[u"statements"])
                        where[u"values"] += ret[u"values"]

                elif key in self._OPERATORS and parent is not None:

                    db_field = self.formated_to_header(parent, use_alias=use_alias, dependencies=dependencies)
                    value = self.get_wrapped_values([db_field], [filter[key]], use_alias=use_alias)
                    where[u"statements"].append(db_field + u" " + self._OPERATORS[key] + u" " + value)
                    where[u"values"].append(filter[key])

                elif key in self._RECURSIVE_OPERATORS:

                    ret = self.parse_filters(
                        filter[key],
                        self._RECURSIVE_OPERATORS[key],
                        parent=key,
                        use_alias=use_alias,
                        dependencies=dependencies
                    )
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
                    self.formated_to_header(field, use_referenced=True),
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

    def to_one_level_json(self, obj, parent=None):
        output = {}
        parent = parent or []

        for key in obj:
            if type(obj[key]) is not dict:
                output[u".".join(parent + [key])] = obj[key]
            else:

                output.update(self.to_one_level_json(obj[key], parent + [key]))

        return output

    def get_wrapped_values(self, headers, values, use_alias=False):

        output = []

        for index, header in enumerate(headers):
            for column in self._columns:
                if use_alias:
                    sep = u"."
                else:
                    sep = u"`.`"

                if (u"`" + column.get(u"alias", column.get(u"table_name")) + sep + column[u"column_name"] + u"`") == header:

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
            (self.formated_to_header(field, use_referenced=True), one_level_data[field])
            for field in one_level_data
            if self._table in self.formated_to_header(field, use_referenced=True)
        ])

        insert[u"statements"] = insert[u"statements"].replace(u"#fields", u", ".join(list(db_fields)))
        insert[u"statements"] = insert[u"statements"].replace(u"#values", self.get_wrapped_values(db_fields, insert[u"values"]))

        return insert


    def generate_dependencies(self, parent_table=None, parent_path=None, filters=None, use_alias=False):

        table = parent_table or self._table
        j_tab = parent_path or []
        alias = table
        if parent_path is not None:
            alias = parent_path[-1]
            
        fields, joins = [], []
        # For each column which doesn't have any relation
        for col in [col for col in self._columns if col.get(u"alias", col.get(u"table_name")) == alias and u"referenced_table_name" not in col]:
           
            fields.append({
                u"db": u"`" + col.get(u"alias") + u"`.`" + col.get(u"column_name") + u"`",
                u"formated": u".".join(j_tab + [col.get(u"column_name")]),
                u"alias": u"`" + col.get(u"alias") + u"." + col.get(u"column_name") + u"`"
            })

        # For each column which has a relation
        for ref_col in [
            col for col in self._columns
            if (u"referenced_table_name" in col and col.get(u"table_name") == table)
        ]:
            new_parent_path = j_tab + [ref_col.get(u"referenced_alias")]
            joins += [ref_col]

            ret = self.generate_dependencies(
                parent_table=ref_col.get(u"referenced_table_name"),
                parent_path=new_parent_path
            )
            fields += ret[0]
            joins += ret[2]


        # Then format JSON
        fields = [
            {
                u"formated": self.headers_to_json([field.get(u"formated")])[0],
                u"db": field.get(u"db"),
                u"alias": field.get(u"alias")
            } for field in fields]



        # Return
        return [
            fields,
            self._table,
            joins,
            self.parse_filters(filters, use_alias=use_alias)
        ]

    def group(self, pattern):
        pass

    def match(self, pattern):
        pass

    def get_stage(self, stage):
        for key in stage:
            if key in self.__stages_binding:
                 return self.__stages_binding[key](stage)

    def aggregate(self, aggregation):
        ret = []

        for stage in aggregation:
            ret.append(self.get_stage(stage))
