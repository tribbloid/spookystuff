import json

from dronekit import LocationGlobalRelative

from pyspookystuff.mav.routing import Binding, Instance


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

    def prepare(self, conf):

        _jInstances = conf['instances']
        instances = map(lambda x: Instance(x), _jInstances)
        self.binding = Binding.getOrCreate(
            instances,
            conf['proxyFactory']
        )



    def moveTo(self, point):
        # type: (LocationGlobalRelative) -> object
        self.binding
