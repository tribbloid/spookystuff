import json

from dronekit import LocationGlobalRelative


class DummyPyAction(object):
    this = None

    def __init__(self, jsonStr):
        # type: (object) -> object
        self.this = json.loads(jsonStr)

    def dummy(self, jsonStr):
        args = json.loads(jsonStr)
        merged = int(self.this['params']['a']['value']) + int(args['b'])
        print(json.dumps(merged))


class Move(object):
    this = None

    def __init__(self, jsonStr):
        # type: (str, str) -> object
        self.this = json.loads(jsonStr)

    def moveTo(self, point):
        # type: (LocationGlobalRelative) -> object
        session = json.loads(sessionJSON)

        session
