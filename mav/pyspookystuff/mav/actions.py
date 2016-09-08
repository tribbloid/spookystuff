import json


class DummyPyAction(object):
    this = None

    def __init__(self, jsonStr):
        # type: (object) -> object
        self.this = json.loads(jsonStr)
        # raise Exception(self.this)

    def dummy(self, jsonStr):
        map = json.loads(jsonStr)
        merged = int(self.this['params']['a']['value']) + int(map['b'])
        print(json.dumps(merged))

class Move(object):

    this = None

    def __init__(self, jsonStr):
        # type: (str, str) -> object
        self.this = json.loads(jsonStr)

    def exe(sessionJSON):
        # type: (str) -> object
        session = json.loads(sessionJSON)

        session
