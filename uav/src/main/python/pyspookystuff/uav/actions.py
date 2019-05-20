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


class DummyPyAction(object):

    def __init__(self, a):
        self.a = a

    def dummy(self, b, c):
        print("DEBUG", self.a, b, c)
        merged = int(self.a) * int(b) * int(c)
        return json.dumps(merged)
