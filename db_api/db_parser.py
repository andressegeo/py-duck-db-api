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

        self._GROUP_OPERATORS = {
            u"$sum": u"SUM",
            u"$avg": u"AVG"
        }

        self._RECURSIVE_OPERATORS = {
            u"$and": u"AND",
            u"$or": u"OR"
        }

        self._base_columns = columns
        self._table = table
        self._last_state = None

    def generate_base_state(self, parent_table=None, parent_path=None):
        """
        This method generate a set of variables, that can be seen as dependencies, used by others function
        to construct the base state of the queries allowed to be applied to the selected table.
        The return value is a dict, that can contains anything relevant (a state). In the case of this method,
        It always contains fields, joins, to construct a simple get request to a table and his relations.
        :param parent_table:
        :param parent_path:
        :param set_last_state: Keep in the attribute _last_state.
        :return: dict
        """
        table = parent_table or self._table
        j_tab = parent_path or []
        alias = table
        if parent_path is not None:
            alias = parent_path[-1]

        fields, joins = [], []
        # For each column which doesn't have any relation
        for col in [col for col in self._base_columns if
                    col.get(u"alias", col.get(u"table_name")) == alias and u"referenced_table_name" not in col]:
            fields.append({
                u"db": u"`" + col.get(u"alias") + u"`.`" + col.get(u"column_name") + u"`",
                u"formated": u".".join(j_tab + [col.get(u"column_name")]),
                u"alias": col.get(u"alias") + u"." + col.get(u"column_name")
            })

        # For each column which has a relation
        for ref_col in [
            col for col in self._base_columns
            if (u"referenced_table_name" in col and col.get(u"table_name") == table)
            ]:
            new_parent_path = j_tab + [ref_col.get(u"referenced_alias")]
            joins += [ref_col]

            ret = self.generate_base_state(
                parent_table=ref_col.get(u"referenced_table_name"),
                parent_path=new_parent_path
            )
            fields += ret.get(u"fields")
            joins += ret.get(u"joins")

        # Then format JSON
        fields = [
            {
                u"formated": field.get(u"formated"),
                u"db": field.get(u"db"),
                u"alias": field.get(u"alias")
            } for field in fields]

        base_state = {
            u"fields": fields,
            u"joins": joins,
            u"type": u"base"
        }
        # Return
        return base_state


    def generate_column_description(self, table, columns):
        types_desc = {
            u"number": [u"int", u"float"],
            u"text": [u"varchar", u"text"],
            u"timestamp": [u"date"]
        }

        ret = []

        def get_machine_type(col):
            matching_type = None
            for key in types_desc:
                for db_type in types_desc[key]:
                    if db_type in col.get(u'type'):
                        matching_type = key
                        break
            return matching_type

        def set_append_and_extra(col, col_desc):
            if col.get(u'key', u"") != u"":
                col_desc[u'key'] = col.get(u'key')

            if col.get(u'extra', u"") != u"":
                col_desc[u'extra'] = col.get(u'extra')
            return col_desc

        for col in [col for col in columns if col[u"table_name"] == table]:
            matching_type = get_machine_type(col)

            if matching_type is None:
                raise ValueError(u"No matching types")

            col_desc = {
                u"name": col.get(u'column_name'),
                u"required": not col.get(u"null"),
                u"type": matching_type
            }

            set_append_and_extra(col, col_desc)

            if u"referenced_table_name" in col:
                nested_fields = [
                    set_append_and_extra(nested_col, {
                        u"name": nested_col.get(u'column_name'),
                        u"required": not nested_col.get(u"null"),
                        u"type": get_machine_type(nested_col)
                    }) for nested_col in columns
                    if col_desc.get(u"name") == nested_col.get(u'alias')
                    ]

                col_desc[u'nestedDescription'] = {
                    u"fields": nested_fields,
                    u"source": col.get(u"referenced_table_name")
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


    def rows_to_formated(self, headers, rows, fields, is_formated=True):
        items = []
        for row in rows:
            item = {}
            for index, cell in enumerate(row):
                key = u"alias"
                if is_formated:
                    key = u"formated"
                item = self.json_put(item, fields[index][key], self.python_type_to_json(cell))

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

    def formated_to_header(self, json_field, use_referenced=False, use_alias=False, from_state=None):
        """
        Format a JSON field given in parameter, when calling the web service attached for example.
        It will format the field in a database friendly string, and try to find it in
        :param json_field:
        :param use_referenced:
        :param use_alias:
        :param from_state:
        :return:
        """
        if from_state is None:
            from_state = self.generate_base_state()

        db_field = None

        for field in from_state.get(u"fields"):
            if field.get(u"formated") == json_field:
                if use_alias:
                    db_field = field.get(u"alias")
                else:
                    db_field = field.get(u"db")
                break

        if use_referenced:
            for ref in from_state.get(u"joins", []):
                if db_field == (u"`" + ref[u"referenced_alias"] + u"`.`" + ref[u"referenced_column_name"] + u"`"):
                    if use_alias:
                        db_field = (u"`" + ref[u"table_name"] + u"." + ref[u"column_name"] + u"`")
                    else:
                        db_field = (u"`" + ref[u"table_name"] + u"`.`" + ref[u"column_name"] + u"`")
                    break
        return db_field

    def get_columns_with_reference(self):
        return [column for column in self._base_columns
            if u"referenced_table_name" in column]

    def parse_match(
            self,
            match,
            from_state,
            operator=u"AND",
            parent=None,
            filter_with_alias=True
    ):
        self._last_state = from_state

        match = match or {}

        if type(match) is not list:
            match = [match]

        where = {
            u"statements": [],
            u"values": []
        }

        for filter in match:

            for key in filter:
                # If key is an operator
                if self.is_field(key):

                    field = self.find_col_field(key)
                    value = self.get_wrapped_values(
                        [field],
                        [filter[key]]
                    )
                    if not filter_with_alias:
                        field = self.find_col_field(key, field_key=u"db")
                    else:
                        field = u"`" + field + u"`"

                    if type(filter[key]) in [unicode, str, int, float]:
                        where[u"statements"].append(field + u" = " + value)
                        where[u"values"].append(filter[key])

                    elif type(filter[key]) is dict:

                        ret = self.parse_match(filter[key], parent=key, from_state=from_state)
                        where[u"statements"].append(ret[u"statements"])
                        where[u"values"] += ret[u"values"]

                elif key in self._OPERATORS and parent is not None:

                    field = self.find_col_field(parent)
                    value = self.get_wrapped_values(
                        [field],
                        [filter[key]]
                    )

                    if not filter_with_alias:
                        field = self.find_col_field(key, field_key=u"db")
                    else:
                        field = u"`" + field + u"`"

                    where[u"statements"].append(field + u" " + self._OPERATORS[key] + u" " + value)
                    where[u"values"].append(filter[key])

                elif key in self._RECURSIVE_OPERATORS:

                    ret = self.parse_match(
                        filter[key],
                        from_state=from_state,
                        operator=self._RECURSIVE_OPERATORS[key],
                        parent=key,
                        filter_with_alias=filter_with_alias
                    )

                    where[u"statements"].append(u"(" + ret[u"statements"] + u")")
                    where[u"values"] += ret[u"values"]

        where[u"statements"] = (u" " + operator + u" ").join(where[u"statements"])

        return where

    def is_field(self, key):
        return self.find_col_field(key) is not None

    def parse_project(self, project, from_state=None):
        self._last_state = from_state
        ret = {
            u"statements": [],
            u"values": [],
            u"state": {
                u"fields": []
            }  # Dependencies for potential next stage
        }

        for key in project:
            if project[key] == 1:
                field = self.find_col_field(key)
                ret[u'statements'].append(u"`" + field + u"` AS %s")
                ret[u'values'].append(key)
                ret[u'state'][u"fields"].append({
                    u"alias": key,
                    u"formated": key
                })
            elif type(project[key]) is unicode and u"$" in project[key]:

                field = self.find_col_field(project[key][1:])
                ret[u'statements'].append(u"`{}` AS %s".format(field))
                ret[u'values'].append(key)
                ret[u'state'][u"fields"].append({
                    u"alias": key,
                    u"formated": key
                })

        ret[u'statements'] = u", ".join(ret[u'statements'])
        return ret

    def find_col_field(self, key, field_key=u"alias"):
        for field in self._last_state.get(u"fields", []):
            if field.get(u"formated", u"") != u"" and key == field.get(u"formated", u""):
                return field.get(field_key)

            for variable in [key, self._table + u"." + key]:
                if variable == field.get(u"alias"):
                    return field.get(field_key)
        return None

    def parse_order_by(self, order_by, from_state):

        order_by = order_by or {}
        self._last_state = from_state
        ret = {
            u"statements": []
        }

        for key in order_by:
            field = self.find_col_field(key)
            formatted = u"`{}` {}"
            if order_by[key] == 1:
                ret[u"statements"].append(formatted.format(field, u"ASC"))
            elif order_by[key] == -1:
                ret[u"statements"].append(formatted.format(field, u"DESC"))

        ret[u'statements'] = u", ".join(ret[u'statements'])
        return ret

    def parse_group(self, group, from_state):

        self._last_state = from_state
        ret = {
            u"fields": [],
            u"group_by": [],
            u"values": [],
            u"state": {
                u"fields": []
            }  # Dependencies for potential next stage
        }

        # Could be ignored, but MongoDB requires it. We stay with this then.
        if u"_id" not in group:
            raise ValueError(u"_id field is mandatory")

        for key in group:
            if key == u"_id" and group[key] is not None:
                group_by = group[key]
                for grp_key in group_by:
                    if type(group_by[grp_key]) is unicode and u"$" == group_by[grp_key][0]:
                        field = self.find_col_field(group_by[grp_key][1:])
                        ret[u"group_by"].append(u"`{}`".format(field))
                        id_field = u"_id.{}".format(field)
                        ret[u"fields"].append(u"`{}` AS `{}`".format(field, id_field))
                        ret[u"state"][u"fields"].append({
                            u"alias": id_field,
                            u"formated": id_field
                        })
            elif type(group[key]) == dict:

                accumulators = group[key]
                for acc_key in accumulators:
                    acc_op = self._GROUP_OPERATORS.get(acc_key, None)
                    acc_val = accumulators.get(acc_key, None)
                    if acc_val == 1:
                        ret[u"fields"].append(u"{}(`{}`) AS `{}`".format(
                            acc_op,
                            u"*",
                            u"%s"
                        ))
                        ret[u"values"].append(key)
                        ret[u"state"][u"fields"].append({
                            u"alias": key,
                            u"formated": key
                        })
                    elif type(acc_val) is unicode and acc_val[0] == u"$":
                        field = self.find_col_field(acc_val[1:])
                        ret[u"fields"].append(u"{}(`{}`) AS {}".format(
                            acc_op,
                            field,
                            u"%s"
                        ))
                        ret[u"values"].append(key)
                        ret[u"state"][u"fields"].append({
                            u"alias": key,
                            u"formated": key
                        })

        return ret


    def to_db_field_selector(self, field):
        if u"." not in field:
            field = self._table + u"." + field
        return u"`" + field + u"`"

    def to_one_level_json(self, obj, parent=None):
        output = {}
        parent = parent or []

        for key in obj:
            if type(obj[key]) is not dict:
                output[u".".join(parent + [key])] = obj[key]
            else:

                output.update(self.to_one_level_json(obj[key], parent + [key]))

        return output

    def get_wrapped_values(self, headers, values):

        output = []

        for index, header in enumerate(headers):
            if self._last_state is not None and self._last_state.get(u"type", u"") != u"base":
                columns = self._last_state.get(u"fields", [])
                for column in columns:
                    if header == column.get(u"alias"):
                        output.append(u"%s")
                        break
            else:
                # If no base state, parse from columns
                for column in self._base_columns:

                    sep = u"."
                    if ((column.get(u"alias", column.get(u"table_name")) + sep + column[u"column_name"])
                            == header):

                        if u"datetime" in column.get(u"type", u"") and type(values[index]) in [int, float]:
                            output.append(u"FROM_UNIXTIME(%s)")
                        else:
                            output.append(u"%s")
                        break
        return u", ".join(output)

    def parse_update(self, data):
        self._last_state = self.generate_base_state()
        update = {
            u"statements": [],
            u"values": []
        }

        if u"$set" in data:
            data = self.to_one_level_json(data[u"$set"])

            # Reformat
            reformated_data = {}
            for key in data:
                if type(data[key]) is dict:
                    reformated_data[key] = data[key][u"id"]
                elif type(data[key]) is not dict and type(key) in [str, unicode] and key.count(u".") == 1:
                    tab = key.split(u".")
                    if len(tab) == 2 and tab[1] == u"id":
                        reformated_data[tab[0]] = data[key]
                else:
                    reformated_data[key] = data[key]

            data = reformated_data
            for key in data:
                for col in self._base_columns:
                    if col.get(u"table_name") == self._table and col.get(u"column_name") == key:
                        db_field = u"{}.{}".format(col.get(u"table_name"), col.get(u"column_name"))
                        wrapped = self.get_wrapped_values([db_field], [data[key]])
                        db_field = u"`{}`.`{}`".format(col.get(u"table_name"), col.get(u"column_name"))
                        update[u"statements"] += [(db_field + u" = " + wrapped)]
                        update[u"values"].append(data[key])

        update[u"statements"] = u", ".join(update[u"statements"])

        if update[u"statements"] != u"":
            update[u"statements"] = u"SET " + update[u"statements"]

        return update

    def parse_insert(self, data):

        self._last_state = self.generate_base_state()

        insert = {
            u"fields": [],
            u"positional_values": [],
            u"values": []
        }

        # Remove external dep
        for key in data:
            if type(data[key]) is dict:
                data[key] = data[key][u"id"]
            for col in self._base_columns:
                if col.get(u"table_name") == self._table and col.get(u"column_name") == key:
                    db_field = u"{}.{}".format(col.get(u"table_name"), col.get(u"column_name"))
                    wrapped = self.get_wrapped_values([db_field], [data[key]])
                    insert[u'fields'].append(
                        u"`{}`.`{}`".format(col.get(u"table_name"), col.get(u"column_name"))
                    )
                    insert[u"positional_values"].append(wrapped)
                    insert[u"values"].append(data[key])
                    break

        return insert