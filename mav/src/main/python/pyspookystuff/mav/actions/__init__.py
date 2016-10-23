import json

from dronekit import LocationGlobalRelative, LocationGlobal

from pyspookystuff.mav import assureInTheAir
from pyspookystuff.mav.comm import Connection, Endpoint, ProxyFactory


class PyAction(object):

    def __init__(self, params, className=None):
        self.params = params

class DummyPyAction(PyAction):

    def dummy(self, b, c):
        print("DEBUG", self.params['a']['value'], b, c)
        merged = int(self.params['a']['value']) * int(b) * int(c)
        return json.dumps(merged)


class DroneAction(PyAction):

    def assureInTheAir(self, _endpoints, _proxyFactory, takeOffAltitude):
        if not self.binding:
            instances = map(lambda v: Endpoint(**v), _endpoints)
            proxyFactory = ProxyFactory(**_proxyFactory)

            # if a binding is already created for this process it will be reused.
            self.binding = Connection.getOrCreate(instances, proxyFactory)

        assureInTheAir(takeOffAltitude, self.binding.vehicle)

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
