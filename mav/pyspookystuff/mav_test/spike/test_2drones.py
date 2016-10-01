from __future__ import print_function

from dronekit import LocationGlobalRelative

from pyspookystuff.mav_test import *

def test_both():

    setup_sitl_mavproxy(options='--out=127.0.0.1:10092', instance=1)
    setup_sitl_mavproxy(options='--out=127.0.0.1:10102', instance=0)

    ferry('127.0.0.1:10092', '127.0.0.1:10102')

    teardownAll()

    return

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

    arm_and_takeoff(20, vehicle1)
    arm_and_takeoff(20, vehicle2)

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
