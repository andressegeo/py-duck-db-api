# -*- coding: utf-8 -*-

import datetime
import calendar
import re
import json
import csv
import StringIO


class DBExport:

    def __init__(self):
        pass

    def export(self, headers, rows, options):
        return self._to_csv(headers, rows, options)

    @staticmethod
    def _to_csv(headers, rows, options):
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