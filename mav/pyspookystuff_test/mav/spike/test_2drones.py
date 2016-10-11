from __future__ import print_function

import time
from dronekit import LocationGlobalRelative, connect, VehicleMode

from pyspookystuff.mav import assureInTheAir
from pyspookystuff_test.mav import sitlProxyUp, sitlProxyClean


def test_both():

    sitlProxyUp(outs=['127.0.0.1:10092'])
    sitlProxyUp(outs=['127.0.0.1:10102'])

    ferry('127.0.0.1:10092', '127.0.0.1:10102')

    sitlProxyClean()


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

    assureInTheAir(20, vehicle1)
    assureInTheAir(20, vehicle2)

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
