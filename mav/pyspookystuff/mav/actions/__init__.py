import json

from dronekit import LocationGlobalRelative, LocationGlobal

from pyspookystuff.mav import assureInTheAir
from pyspookystuff.mav.routing import Binding, Instance


class PyAction(object):

    def __init__(self, thisStr):
        # type: (object) -> object
        self.this = json.loads(thisStr)
        self.binding = None


class DummyPyAction(PyAction):

    def dummy(self, argStr):
        args = json.loads(argStr)
        merged = int(self.this['params']['a']['value']) + int(args['b'])
        return json.dumps(merged)


class DroneAction(PyAction):

    def assureInTheAir(self, mavConf): # mavConf is always a dict
        if not self.binding:
            _instances = mavConf['instances']
            instances = map(lambda x: Instance(x), _instances)

            # if a binding is already created for this process it will be reused.
            self.binding = Binding.getOrCreate(
                instances,
                mavConf['proxyFactory']
            )

        assureInTheAir(mavConf['takeOffAltitude'], self.binding.vehicle)

    def before(self, mavConfStr):
        pass

    def bulk(self, mavConfStr):
        pass

    def after(self, mavConfStr):
        pass


def toLocation(dict):
    location = dict['globalLocation']
    lat = location['lat']
    lon = location['lon']
    alt = location['alt']

    if location['relative']:
        return LocationGlobalRelative(lat, lon, alt)
    else:
        return LocationGlobal(lat, lon, alt)


class Move(DroneAction):

    def before(self, mavConfStr):
        mavConf = json.loads(mavConfStr)
        assureInTheAir(mavConf)

        frm = toLocation(self.this['from'])
        self.binding.vehicle.simple_goto(frm)

    def bulk(self, mavConfStr):
        to = toLocation(self.this['to'])
        self.binding.vehicle.simple_goto(to)
