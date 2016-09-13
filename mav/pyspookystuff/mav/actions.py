import json

from dronekit import LocationGlobalRelative

from pyspookystuff.mav import arm_and_takeoff
from pyspookystuff.mav.routing import Binding, Instance


class PyAction(object):

    def __init__(self, jThis):
        # type: (object) -> object
        self.this = json.loads(jThis)
        self.binding = None

    def prepareMAV(self, mavConf):

        if not self.binding:
            _jInstances = mavConf['instances']
            instances = map(lambda x: Instance(x), _jInstances)

            # if a binding is already created for this process it will be reused.
            self.binding = Binding.getOrCreate(
                instances,
                mavConf['proxyFactory']
            )

        if not self.binding.vehicle:
            arm_and_takeoff(mavConf['takeOffAltitude'], self.binding.vehicle)


class DummyPyAction(PyAction):

    def dummy(self, jsonStr):
        args = json.loads(jsonStr)
        merged = int(self.this['params']['a']['value']) + int(args['b'])
        print(json.dumps(merged))


class Move(PyAction):

    def goTo(self, point):
        # type: (LocationGlobalRelative) -> None
        self.binding.vehicle.goto(point)
