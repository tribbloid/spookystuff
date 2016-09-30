import json

from dronekit import LocationGlobalRelative

from pyspookystuff.mav import arm_and_takeoff
from pyspookystuff.mav.routing import Binding, Instance


class PyAction(object):

    def __init__(self, _json):
        # type: (object) -> object
        self.this = json.loads(_json)
        self.binding = None


class DummyPyAction(PyAction):

    def dummy(self, jsonStr):
        args = json.loads(jsonStr)
        merged = int(self.this['params']['a']['value']) + int(args['b'])
        print(json.dumps(merged))


class DroneAction(PyAction):

    def prepareMAV(self, mavConf):
        if not self.binding:
            _instances = mavConf['instances']
            instances = map(lambda x: Instance(x), _instances)

            # if a binding is already created for this process it will be reused.
            self.binding = Binding.getOrCreate(
                instances,
                mavConf['proxyFactory']
            )

        # how do you know if its in the ground
        inTheAir = self.binding.vehicle.is_armable

        if not self.binding.vehicle:
            arm_and_takeoff(mavConf['takeOffAltitude'], self.binding.vehicle)


class Move(DroneAction):

    def goTo(self, point):
        # type: (LocationGlobalRelative) -> None
        self.binding.vehicle.goto(point)
