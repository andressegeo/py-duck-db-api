# -*- coding: utf-8 -*-


class Parser:

    def __init__(self, table_name, columns):
        self.__stages_binding = {
            u"$match": self.match,
            u"$group": self.group
        }

    def group(self, pattern):
        pass

    def match(self, pattern):
        return u"""SELECT * FROM hour"""

    def get_stage(self, stage):
        for key in stage:
            if key in self.__stages_binding:
                 return self.__stages_binding[key](stage)

    def aggregate(self, aggregation):
        ret = []

        for stage in aggregation:
            ret.append(self.get_stage(stage))

        print(ret)

parser = Parser()

parser.aggregate([
    {
        u"$match": {}
    }
])



