import json

'''
crash course on APM & PX4 flight modes:
APM:
Stabilize
Alt Hold
Loiter
RTL (Return-to-Launch)
Auto
Additional flight modes:

Acro
AutoTune
Brake
Circle
Drift
Guided (and Guided_NoGPS)
Land
PosHold
Sport
Throw
Follow Me
Simple and Super Simple
Avoid_ADSB for ADS-B based avoidance of manned aircraft. Should not be set-up as a pilot selectable flight mode.

PX4:

MANUAL
  Fixed wing aircraft/ rovers / boats:
    MANUAL
    STABILIZED

  Multirotors:
    ACRO
    RATTITUDE
    ANGLE

ASSISTED
  ALTCTL
  POSCTL

AUTO
  AUTO_LOITER
  AUTO_RTL
  AUTO_MISSION
'''

from dronekit import LocationGlobalRelative, LocationGlobal

from pyspookystuff.mav import assureInTheAir
from pyspookystuff.mav.telemetry import Link, Endpoint

class DummyPyAction(object):

    def __init__(self, a):
        self.a = a

    def dummy(self, b, c):
        print("DEBUG", self.a, b, c)
        merged = int(self.a) * int(b) * int(c)
        return json.dumps(merged)


# class DroneAction(PyAction):
#
#     def assureInTheAir(self, _endpoints, proxy, takeOffAltitude):
#         if not self.binding:
#             instances = map(lambda v: Endpoint(**v), _endpoints)
#
#             # if a binding is already created for this process it will be reused.
#             self.binding = Link.getOrCreate(instances, proxy)
#
#         assureInTheAir(takeOffAltitude, self.binding.vehicle)
#
#     def before(self, mavConfStr):
#         pass
#
#     def bulk(self, mavConfStr):
#         pass
#
#     def after(self, mavConfStr):
#         pass
#
#
# def toLocation(dict):
#     location = dict['globalLocation']
#     lat = location['lat']
#     lon = location['lon']
#     alt = location['alt']
#
#     if location['relative']:
#         return LocationGlobalRelative(lat, lon, alt)
#     else:
#         return LocationGlobal(lat, lon, alt)
#
#
# class Move(DroneAction):
#
#     def before(self, mavConfStr):
#         mavConf = json.loads(mavConfStr)
#         assureInTheAir(mavConf)
#
#         frm = toLocation(self.this['from'])
#         self.binding.vehicle.simple_goto(frm)
#
#     def bulk(self, mavConfStr):
#         to = toLocation(self.this['to'])
#         self.binding.vehicle.simple_goto(to)
