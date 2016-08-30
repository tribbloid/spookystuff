from __future__ import print_function

import time
from dronekit import connect, VehicleMode, LocationGlobalRelative
from nose.tools import assert_equals

from python.test import *

def test_both():

    setup_sitl_mavproxy(options='--out=127.0.0.1:10092', instance=1)
    setup_sitl_mavproxy(options='--out=127.0.0.1:10102', instance=0)

    ferry('127.0.0.1:10092', '127.0.0.1:10102')

    teardown_sitl()

    return

# def set_sysid(v, instance):


def ferry(path1, path2):
    vehicle1 = connect(path1, wait_ready=True)
    vehicle2 = connect(path2, wait_ready=True)

    # NOTE these are *very inappropriate settings*
    # to make on a real vehicle. They are leveraged
    # exclusively for simulation. Take heed!!!
    vehicle1.parameters['FS_GCS_ENABLE'] = 0
    vehicle1.parameters['FS_EKF_THRESH'] = 100

    vehicle2.parameters['FS_GCS_ENABLE'] = 0
    vehicle2.parameters['FS_EKF_THRESH'] = 100

    print("Allowing time for parameter write")

    def arm_and_takeoff(vehicle, targetAltitude):
        """
        Arms vehicle and fly to aTargetAltitude.
        """

        # Don't let the user try to fly autopilot is booting
        i = 60
        while not vehicle.is_armable and i > 0:
            time.sleep(1)
            i = i - 1
        assert_equals(vehicle.is_armable, True)

        # Copter should arm in GUIDED mode
        vehicle.mode = VehicleMode("GUIDED")
        i = 60
        while vehicle.mode.name != 'GUIDED' and i > 0:
            print(" Waiting for guided %s seconds..." % (i,))
            time.sleep(1)
            i = i - 1
        assert_equals(vehicle.mode.name, 'GUIDED')

        # Arm copter.
        vehicle.armed = True
        i = 60
        while not vehicle.armed and vehicle.mode.name == 'GUIDED' and i > 0:
            print(" Waiting for arming %s seconds..." % (i,))
            time.sleep(1)
            i = i - 1
        assert_equals(vehicle.armed, True)

        # Take off to target altitude
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

            assert_equals(vehicle.mode.name, 'GUIDED')
            time.sleep(1)

    arm_and_takeoff(vehicle1, 20)
    arm_and_takeoff(vehicle2, 20)

    point1 = LocationGlobalRelative(90, 0, 20)
    point2 = LocationGlobalRelative(-90, 0, 20)

    print("Going to first point...")
    vehicle1.simple_goto(point1)

    print("Going to first point...")
    vehicle2.simple_goto(point2)

    # sleep so we can see the change in map
    time.sleep(3000000)

    print("Returning to Launch")
    vehicle1.mode = VehicleMode("RTL")

    vehicle1.close()
