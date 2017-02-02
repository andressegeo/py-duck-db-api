# -*- coding: utf-8 -*-

import datetime
import calendar
import re
import json
import decimal

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

    def determine_type(self, col):
        types_desc = {
            u"number": [u"int", u"float"],
            u"text": [u"varchar", u"text"],
            u"timestamp": [u"date"]
        }

        def get_machine_type(col):
            matching_type = None
            for key in types_desc:
                for db_type in types_desc[key]:
                    if db_type in col.get(u'type'):
                        matching_type = key
                        break
            return matching_type

        return get_machine_type(col)

    def generate_joins(self, table=None, parent_path=None):
        table = table or self._table
        joins = []
        parent_path = parent_path or [table]
        for col in [col for col in self._base_columns if u"referenced_table_name" in col and col.get(u"table_name") == table]:

            from_path = parent_path
            to_path = parent_path + [col.get(u"column_name")]
            already_processed = len(
                [join for join in joins if u".".join(join.get(u"to_path")) == u".".join(to_path)]
            ) > 0

            if not already_processed:
                joins.append({
                    u"from_table": col.get(u"table_name"),
                    u"from_column": col.get(u"column_name"),
                    u"to_table": col.get(u"referenced_table_name"),
                    u"to_column": col.get(u"referenced_column_name"),
                    u"from_path": from_path,
                    u"to_path": parent_path + [col.get(u"column_name")]
                })

        updated_joins = []
        for join in joins:
            updated_joins += self.generate_joins(
                join.get(u"to_table"),
                parent_path + [join.get(u"from_column")]
            )
        return joins + updated_joins

    def generate_fields(self, table=None, parent_path=None, fields=None):

        table = table or self._table
        fields = fields or []
        parent_path = parent_path or [table]
        for col in [col for col in self._base_columns if col.get(u"table_name") == table]:

            if u"referenced_table_name" not in col:
                path = parent_path + [col.get(u"column_name")]
                already_processed = len([
                    field for field in fields
                    if u".".join(field.get(u"path") + [field.get(u"name")]) == u".".join(path)]
                ) > 0

                if not already_processed:
                    fields.append({
                        u"name": col.get(u"column_name"),
                        u"path": parent_path,
                        u"type": self.determine_type(col)
                    })
            else:
                fields = self.generate_fields(
                    col.get(u"referenced_table_name"),
                    parent_path + [col.get(u"column_name")],
                    fields=fields
                )

        return fields

    def generate_base_state(self):
        """
        This method generate a set of variables, that can be seen as dependencies, used by others function
        to construct the base state of the queries allowed to be applied to the selected table.
        The return value is a dict, that can contains anything relevant (a state). In the case of this method,
        It always contains fields, joins, to construct a simple get request to a table and his relations.
        :return: dict
        """
        joins = self.generate_joins()

        fields = self.generate_fields(self._table, [self._table])

        base_state = {
            u"fields": sorted(fields, key=lambda x: len(x.get(u"path"))),
            u"joins": sorted(joins, key=lambda x: len(x.get(u"from_path"))),
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

        if isinstance(val, decimal.Decimal):
            return float(val)
        elif type(val) in [long, float]:
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
            for index, header in enumerate(headers):
                header = header.split(u".")[1:] if self._last_state.get(u"type") == u"base" else header.split(u".")
                print(self._last_state.get(u"type"))
                value = self.python_type_to_json(row[index])
                item = self.json_put(item, u".".join(header), value)
            items.append(item)
        return items

    def get_field(self, path):

        implicit_table = self._last_state.get(u"type", None) == u"base"
        looked_field_path = path
        if implicit_table and self._table != looked_field_path[0]:
            looked_field_path = [self._table] + looked_field_path

        for col in self._last_state.get(u"fields", []):
            col_field_path = col.get(u"path") + [col.get(u"name")]
            if col_field_path == looked_field_path:
                return col
        return None

    def parse_match(
            self,
            match,
            from_state,
            operator=u"AND",
            parent=None,
            use_alias=True
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
                field = self.get_field(path=key.split(u"."))
                if field is not None:
                    value = self.get_wrapped_value(
                        filter[key],
                        field.get(u"type")
                    )

                    if type(filter[key]) is dict:
                        ret = self.parse_match(filter[key], parent=field, from_state=from_state)
                        where[u"statements"].append(ret[u"statements"])
                        where[u"values"] += ret[u"values"]
                    else:

                        # If use alias, we filter from the table name dot column name, and not from the
                        # alias given in the SELECT.
                        # Necessary for Update query which doesn't allow to put alias.
                        # Same thing some line above this.
                        if use_alias:
                            where[u"statements"].append(u"`{}` = {}".format(
                                u".".join(field.get(u"path") + [field.get(u"name")]),
                                str(value))
                            )
                        else:
                            where[u"statements"].append(u"`{}`.`{}` = {}".format(
                                u".".join(field.get(u"path")),
                                field.get(u"name"),
                                str(value))
                            )

                        where[u"values"].append(filter[key])

                elif key in self._OPERATORS and parent is not None:
                    field = self.get_field(parent.get(u"path") + [parent.get(u"name")])
                    value = self.get_wrapped_value(filter[key], field.get(u"type"))

                    if use_alias:
                        where[u"statements"].append(u"`{}` {} {}".format(
                            u".".join(field.get(u"path") + [field.get(u"name")]),
                            self._OPERATORS[key],
                            str(value)
                            )
                        )
                    else:
                        where[u"statements"].append(u"`{}`.`{}` = {}".format(
                            u".".join(field.get(u"path")),
                            self._OPERATORS[key],
                            str(value)
                        ))

                    where[u"values"].append(filter[key])

                elif key in self._RECURSIVE_OPERATORS:
                    ret = self.parse_match(
                        filter[key],
                        from_state=from_state,
                        operator=self._RECURSIVE_OPERATORS[key],
                        parent=None
                    )

                    where[u"statements"].append(u"(" + ret[u"statements"] + u")")
                    where[u"values"] += ret[u"values"]

        where[u"statements"] = (u" " + operator + u" ").join(where[u"statements"])

        return where

    def is_field(self, key):
        return self.get_alias_from_j_path(key) is not None

    def parse_project(self, project, from_state=None):
        self._last_state = from_state
        ret = {
            u"statements": [],
            u"values": [],
            u"state": {
                u"fields": []
            }  # Dependencies for potential next stage
        }
        project = self.to_one_level_json(project) or {}
        for key in project:
            if project[key] == 1:
                field = self.get_field(key.split(u"."))
            elif type(project[key]) is unicode and u"$" in project[key]:
                field = self.get_field(project[key][1:].split(u"."))
            else:
                continue

            ret[u'statements'].append(u"`{}` AS %s".format(u".".join(field.get(u"path") + [field.get(u"name")])))
            ret[u'values'].append(key)
            new_key = key.split(u".")
            ret[u'state'][u"fields"].append({
                u"path": new_key[:-1],
                u"type": field.get(u"type"),
                u"name": new_key[-1]
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

        order_by = self.to_one_level_json(order_by) or {}
        self._last_state = from_state

        ret = {
            u"statements": []
        }

        for key in order_by:
            field = self.get_field(key.split(u"."))
            if order_by[key] in [1, -1]:
                order = u"ASC" if order_by[key] == 1 else u"DESC"
                ret[u"statements"].append(u"`{}` {}".format(u".".join(field.get(u"path") + [field.get(u"name")]), order))

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
                        field = self.get_field(grp_key.split(u"."))
                        ret[u"group_by"].append(u"`{}`".format(u".".join(field.get(u"path") + [field.get(u"name")])))

                        ret[u"fields"].append(u"`{}` AS `{}`".format(
                            u".".join(field.get(u"path") + [field.get(u"name")]),
                            u".".join([u"_id"] + [grp_key])
                        ))

                        new_key = [u"_id"] + grp_key.split(u".")
                        ret[u'state'][u"fields"].append({
                            u"path": new_key[:-1],
                            u"type": field.get(u"type"),
                            u"name": new_key[-1]
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
                        new_key = key.split(u".")
                        ret[u'state'][u"fields"].append({
                            u"path": new_key[:-1],
                            u"type": field.get(u"type"),
                            u"name": new_key[-1]
                        })
                    elif type(acc_val) is unicode and acc_val[0] == u"$":
                        field = self.get_field(acc_val[1:].split(u"."))
                        ret[u"fields"].append(u"{}(`{}`) AS {}".format(
                            acc_op,
                            u".".join(field.get(u"path") + [field.get(u"name")]),
                            u"%s"
                        ))
                        ret[u"values"].append(key)
                        new_key = key.split(u".")
                        ret[u'state'][u"fields"].append({
                            u"path": new_key[:-1],
                            u"type": field.get(u"type"),
                            u"name": new_key[-1]
                        })

        return ret


    def to_db_field_selector(self, field):
        if u"." not in field:
            field = self._table + u"." + field
        return u"`" + field + u"`"

    def to_one_level_json(self, obj, parent=None):
        output = {}
        obj = obj or {}
        parent = parent or []

        for key in obj:
            if type(obj[key]) is not dict:
                output[u".".join(parent + [key])] = obj[key]
            else:

                output.update(self.to_one_level_json(obj[key], parent + [key]))

        return output

    def get_wrapped_value(self, value, typ):
        value = u"%s"
        if typ == u"timestamp":
            value = u"FROM_UNIXTIME(%s)"
        return value

    def parse_update(self, data):
        self._last_state = self.generate_base_state()
        update = {
            u"statements": [],
            u"values": []
        }

        if u"$set" in data:
            data = self.to_one_level_json(data[u"$set"])
            for key in data:
                field = self.get_field(key.split(u"."))
                if field is not None:
                    positional_value = self.get_wrapped_value(data[key], field.get(u"type"))
                    if len(field.get(u"path")) == 1:
                        update[u"statements"].append(u"`{}`.`{}` = {}".format(
                            field.get(u"path")[0],
                            field.get(u"name"),
                            positional_value
                        ))
                    elif len(field.get(u"path")) == 2:
                        update[u"statements"].append(u"`{}`.`{}` = {}".format(
                            field.get(u"path")[0],
                            field.get(u"path")[1],
                            positional_value
                        ))

                    if len(field.get(u"path")) <= 2:
                        update[u"values"].append(data[key])

        return update

    def parse_insert(self, data):

        self._last_state = self.generate_base_state()
        data = self.to_one_level_json(data)

        insert = {
            u"fields": [],
            u"positional_values": [],
            u"values": []
        }

        for key in data:
            field = self.get_field(key.split(u"."))
            if field is not None:
                positional_value = self.get_wrapped_value(data[key], field.get(u"type"))
                if len(field.get(u"path")) == 1:
                    insert[u"fields"].append(u"`{}`.`{}`".format(
                        field.get(u"path")[0],
                        field.get(u"name")
                    ))
                elif len(field.get(u"path")) == 2:
                    insert[u"fields"].append(u"`{}`.`{}`".format(
                        field.get(u"path")[0],
                        field.get(u"path")[1]
                    ))
                if len(field.get(u"path")) <= 2:
                    insert[u'positional_values'].append(positional_value)
                    insert[u"values"].append(data[key])
        return insert

