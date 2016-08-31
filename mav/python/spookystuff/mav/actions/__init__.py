import json

class DummyPyAction(object):

    this = None

    def __init__(self, jsonStr):
        # type: (object) -> object
        self.this = json.loads(jsonStr)

    def exe(self, jsonStr):
        map = json.loads(jsonStr)
        return json.dump(self.this + map)