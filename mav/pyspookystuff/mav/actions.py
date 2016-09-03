import json


class DummyPyAction(object):
    this = None

    def __init__(self, jsonStr):
        # type: (object) -> object
        self.this = json.loads(jsonStr)

    def dummy(self, jsonStr):
        map = json.loads(jsonStr)
        return json.dump(self.this + map)

class Move(object):

    this = None

    def __init__(self, actionJSON):
        # type: (str, str) -> object
        self.this = json.loads(actionJSON)

    def exe(sessionJSON):
        # type: (str) -> object
        session = json.loads(sessionJSON)

        session
