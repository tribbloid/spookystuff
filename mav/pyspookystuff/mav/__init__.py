from __future__ import print_function

import logging
import os

import time
from dronekit import VehicleMode
from multiprocessing import Lock, Manager

from pyspookystuff import PyspookyException


manager = Manager()


lock = Lock()


# existing has to be thread safe
def nextUnused(existing, candidates, blacklist=list()):
    global lock

    lock.acquire()
    combined = existing + blacklist
    for i in candidates:
        if i not in combined:
            existing.append(i)
            lock.release()
            return i

    lock.release()
    os.error("Depleted: running dry!")


class DronePoolDepletedException(PyspookyException):
    pass


def assureInTheAir(targetAlt, vehicle):
    alt = vehicle.location.global_relative_frame.alt
    if alt > targetAlt:
        logging.info("already in the air")
    else:
        armIfNot(vehicle)
        blockingTakeoff(targetAlt, vehicle)


def armIfNot(vehicle):
    if not vehicle.armed:
        blockingArm(vehicle)


def blockingArm(vehicle):
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


def blockingTakeoff(targetAltitude, vehicle):

    vehicle.simple_takeoff(targetAltitude)

    # Wait until the vehicle reaches a safe height before
    # processing the goto (otherwise the command after
    # Vehicle.simple_takeoff will execute immediately).
    while True:
        print(" Altitude: ", vehicle.location.global_relative_frame.alt)
        # Test for altitude just below target, in case of undershoot.
        if vehicle.location.global_relative_frame.alt >= targetAltitude * 0.95:
            print("Reached target altitude")
            break

        time.sleep(1)
