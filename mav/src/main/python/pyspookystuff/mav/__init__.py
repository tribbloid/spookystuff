from __future__ import print_function

import logging

import time
from dronekit import LocationGlobal, LocationGlobalRelative, LocationLocal
from dronekit import Vehicle, VehicleMode
from pymavlink import mavutil

from pyspookystuff.mav import Const
from pyspookystuff.mav.utils import retry


class MAVException(Exception):
    pass


class UnsupportedException(Exception):
    pass


# doesn't offer too much benefit
class VehicleFunctions(object):

    def __init__(self, vehicle):
        # type: (Vehicle) -> None
        self.vehicle = vehicle


def defaultError(alt=10000.0):
    error = min(alt*0.05, 1)
    return error


# all the following are blocking API
@retry(Const.armRetries)
def assureInTheAir(vehicle, targetAlt, error=None):
    # type: (Vehicle, float, float) -> None
    if not error:
        error = defaultError(targetAlt)

    alt = vehicle.location.global_relative_frame.alt
    if abs(alt - targetAlt) <= error:
        logging.info("already airborne")
    else:
        armAndTakeoff(targetAlt, vehicle, error)


def armAndTakeoff(targetAlt, vehicle, error):
    # type: (float, Vehicle, float) -> None
    '''
    from http://python.dronekit.io/develop/best_practice.html
    Launch sequence
    Generally you should use the standard launch sequence described in Taking Off:
    Poll on Vehicle.is_armable until the vehicle is ready to arm.
    Set the Vehicle.mode to GUIDED
    Set Vehicle.armed to True and poll on the same attribute until the vehicle is armed.
    Call Vehicle.simple_takeoff with a target altitude.
    Poll on the altitude and allow the code to continue only when it is reached.
    The approach ensures that commands are only sent to the vehicle when it is able to act on them
    (e.g. we know Vehicle.is_armable is True before trying to arm, we know Vehicle.armed is True before we take off).
    It also makes debugging takeoff problems a lot easier.
    '''
    # Wait until the vehicle reaches a safe height before
    # processing the goto (otherwise the command after
    # Vehicle.simple_takeoff will execute immediately).

    def armIfNot(vehicle):
        if not vehicle.armed:
            arm(vehicle)

    def arm(vehicle):
        # type: (Vehicle) -> None
        # Don't let the user try to fly when autopilot is booting
        i = 60
        while not vehicle.is_armable and i > 0:
            time.sleep(1)
            i -= 1

        # Copter should arm in GUIDED mode
        vehicle.mode = VehicleMode("GUIDED")
        i = 60
        while vehicle.mode.name != 'GUIDED' and i > 0:
            print(" Waiting for guided %s seconds..." % (i,))
            time.sleep(1)
            i -= 1

        # Arm copter.
        vehicle.armed = True
        i = 60
        while not vehicle.armed and vehicle.mode.name == 'GUIDED' and i > 0:
            print(" Waiting for arming %s seconds..." % (i,))
            time.sleep(1)
            i -= 1

    while True:
        alt = vehicle.location.global_relative_frame.alt
        altPlusError = alt + error

        if alt <= 0.1:
            armIfNot(vehicle)
            print("taking off from the ground ... ")
            vehicle.simple_takeoff(targetAlt)

        # Test for altitude just below target, in case of undershoot.
        if abs(alt - targetAlt) <= error:
            print("Reached target altitude")
            break
        elif alt > targetAlt:
            print("Lowering altitude")
            move(vehicle, LocationGlobalRelative(None, None, targetAlt))

        print(" Altitude: ", alt)
        failOnTimeout(vehicle)
        time.sleep(1)


def homeLocation(vehicle):
    frame = mavutil.mavlink.MAV_FRAME_GLOBAL_RELATIVE_ALT
    if not vehicle.home_location:
        vehicle.commands.download()
        vehicle.commands.wait_ready()
    return vehicle.home_location


def move(vehicle, targetLocation):
    # type: (Vehicle, LocationGlobal) -> None
    # type: (Vehicle, LocationGlobalRelative) -> None
    # type: (Vehicle, LocationLocal) -> None
    """
    does a lot of things:
    block until reaching target location
    if mode!=GUIDED will sleep until mode==GUIDED or reaching target
    will issue simple_goto immediately once mode becomes GUIDED, will issue repeatedly if distance is not closing

    :param vehicle: a vessel of spirit ...
    :param targetLocation: can be LocationGlobal or LocationGlobalRelative.
        if any member==None will copy current vehicle's location into the missing part, feel free to set altitude along
    """
    #TODO: support LocationLocal!

    effectiveTL = targetLocation

    if isinstance(targetLocation, LocationLocal):
        north = targetLocation.north
        east = targetLocation.east
        down = targetLocation.down
        hl = homeLocation(vehicle)
        effectiveTL = utils.get_location_metres(hl, north, east)
        effectiveTL.alt = hl.alt - down

    def currentLocation():
        if isinstance(targetLocation, LocationGlobal):
            return vehicle.location.global_frame
        elif isinstance(targetLocation, LocationGlobalRelative):
            return vehicle.location.global_relative_frame
        elif isinstance(targetLocation, LocationLocal):
            return vehicle.location.global_frame
        else:
            raise UnsupportedException("Only support Dronekit Locations (Global/GlobalRelative/Local)")

    if not effectiveTL.lat:
        effectiveTL.lat = currentLocation().lat
    if not effectiveTL.lon:
        effectiveTL.lon = currentLocation().lon
    if not effectiveTL.alt:
        effectiveTL.alt = currentLocation().alt

    oldDistance = -1.0

    while True: #Stop action if we are no longer in guided mode.
        distance=utils.airDistance(currentLocation(), effectiveTL)
        if distance <= defaultError(): #Just below target, in case of undershoot.
            print("Reached target")
            break

        if vehicle.mode.name=="GUIDED":
            if oldDistance <= distance:
                vehicle.simple_goto(effectiveTL)
                print("Engaging thruster")
            oldDistance = distance
            print("Moving ... distance to target: ", str(distance) + "m")
        else:
            print("relinquished control to manual")
            oldDistance = -1.0

        failOnTimeout(vehicle)
        time.sleep(1)


def failOnTimeout(vehicle):
    assert(vehicle.last_heartbeat < 10)

