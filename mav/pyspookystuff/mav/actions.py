import json


class DummyPyAction(object):
    this = None

    def __init__(self, jsonStr):
        # type: (object) -> object
        self.this = json.loads(jsonStr)[0]

    def dummy(self, jsonStr):
        map = json.loads(jsonStr)
        merged = dict(list(self.this.items()) + list(map.items()))
        print(json.dumps(merged))

class Move(object):

    this = None

    def __init__(self, jsonStr):
        # type: (str, str) -> object
        self.this = json.loads(jsonStr)[0]

    def exe(sessionJSON):
        # type: (str) -> object
        session = json.loads(sessionJSON)

        session
