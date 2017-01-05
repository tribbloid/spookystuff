from __future__ import print_function

import os

import math
import time
from dronekit import LocationGlobal, LocationGlobalRelative
from math import radians, cos, sin, asin, sqrt

earth_radius = 6378137.0  # Radius of "spherical" earth


DEVNULL = open(os.devnull, 'w')


def _groundDistance(lon1, lat1, lon2, lat2):
    """
    haversine formula:
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return c * earth_radius


def groundDistance(p1, p2):
    return _groundDistance(p1.lon, p1.lat, p2.lon, p2.lat)


# TODO: INACCURATE! not considering curvature
def airDistance(p1, p2):
    # type: (LocationGlobal, LocationGlobal) -> (float, float, float)
    haversine = groundDistance(p1, p2)
    altDist = p2.alt - p1.alt
    result = sqrt(haversine * haversine + altDist * altDist)
    return result, haversine, abs(altDist)


def retry(maxTrial=3):
    def decorate(fn):
        def retryFn(*args, **kargs):
            for i in range(maxTrial, -1, -1):
                try:
                    result = fn(*args, **kargs)
                    return result
                except BaseException as e:
                    if i <= 1:
                        raise
                    else:
                        print("Retrying locally on", str(e), "...", str(i - 1), "time(s) left")
                        continue

        return retryFn

    return decorate


def get_location_metres(original_location, dNorth, dEast):
    """
    from http://python.dronekit.io/guide/copter/guided_mode.htmlhttp://python.dronekit.io/guide/copter/guided_mode.html

    Returns a LocationGlobal object containing the latitude/longitude `dNorth` and `dEast` metres from the
    specified `original_location`. The returned LocationGlobal has the same `alt` value
    as `original_location`.

    The function is useful when you want to move the vehicle around specifying locations relative to
    the current vehicle position.

    The algorithm is relatively accurate over small distances (10m within 1km) except close to the poles.

    For more information see:
    http://gis.stackexchange.com/questions/2951/algorithm-for-offsetting-a-latitude-longitude-by-some-amount-of-meters
    """
    # Coordinate offsets in radians
    dLat = dNorth / earth_radius
    dLon = dEast / (earth_radius * math.cos(math.pi * original_location.lat / 180))

    # New position in decimal degrees
    newlat = original_location.lat + (dLat * 180 / math.pi)
    newlon = original_location.lon + (dLon * 180 / math.pi)
    if type(original_location) is LocationGlobal:
        targetlocation = LocationGlobal(newlat, newlon, original_location.alt)
    elif type(original_location) is LocationGlobalRelative:
        targetlocation = LocationGlobalRelative(newlat, newlon, original_location.alt)
    else:
        raise Exception("Invalid Location object passed")

    return targetlocation


def waitFor(condition, duration=60):
    # type: (function, int) -> None
    for i in range(1, duration):
        v = condition(i)
        try:
            v = v[0]
            comment = v[1]
        except:
            comment = ""

        if v:
            return
        time.sleep(1)
        if i%10 == 0:
            print("waiting for", condition.func_name, "\t|", i, "second(s)", comment)
    raise os.error("timeout waiting for " + condition.func_name)

    # not accurate! should use ground dist
    # def get_distance_m(aLocation1, aLocation2):
    #     """
    #     Returns the ground distance in metres between two LocationGlobal objects.
    #
    #     This method is an approximation, and will not be accurate over large distances and close to the
    #     earth's poles. It comes from the ArduPilot test code:
    #     https://github.com/diydrones/ardupilot/blob/master/Tools/autotest/common.py
    #     """
    #     dlat = aLocation2.lat - aLocation1.lat
    #     dlong = aLocation2.lon - aLocation1.lon
    #     return math.sqrt((dlat*dlat) + (dlong*dlong)) * 1.113195e5
    #
    # def get_distance_m(aLocation1, aLocation2):
    #     """
    #     Returns the ground distance in metres between two LocationGlobal objects.
    #
    #     This method is an approximation, and will not be accurate over large distances and close to the
    #     earth's poles. It comes from the ArduPilot test code:
    #     https://github.com/diydrones/ardupilot/blob/master/Tools/autotest/common.py
    #     """
    #     dlat = aLocation2.lat - aLocation1.lat
    #     dlong = aLocation2.lon - aLocation1.lon
    #     return math.sqrt((dlat*dlat) + (dlong*dlong)) * 1.113195e5
