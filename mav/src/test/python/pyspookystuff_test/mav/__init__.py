# tests in this package are only tested locally but not on CI server, they should be lightweight.

from __future__ import print_function

import os
from unittest import TestCase

import dronekit
import time

from pyspookystuff.mav import VehicleFunctions
from pyspookystuff.mav.sim import APMSim
from pyspookystuff.mav.telemetry import Proxy
from pyspookystuff.mav.utils import retry


class TestUtils(TestCase):
    def test_retryCanFail(self):
        """should throw the same exception"""
        # TODO: test counter

        @retry(3)
        def alwaysError():
            print(1/0)

        try:
            alwaysError()
        except:
            return

        raise os.error("impossible")


class TestAPMSim(TestCase):
    dkSSID = 250
    dkBaud = 57600

    def stressTestDownloadWP(self, connStr):
        for i in range(1, 12):
            vehicle = dronekit.connect(
                connStr,
                wait_ready=True,
                source_system=TestAPMSim.dkSSID,
                baud=TestAPMSim.dkBaud
            )

            @retry(2)
            def blockingDownload():
                vehicle.commands.download()
                vehicle.commands.wait_ready()
            blockingDownload()

            vehicle.close()

    def getSim(self):
        sim = APMSim(0, "43.694195,-79.262262,136,353", TestAPMSim.dkBaud)
        return sim, sim.connStr

    def test_downloadWaypointCannotTimeout(self):
        sim, connStr = self.getSim()
        self.stressTestDownloadWP(connStr)

    def test_downloadWaypointThroughProxyCannotTimeout(self):
        sim, connStr = self.getSim()
        proxy = Proxy(connStr, ["udp:localhost:12052", "udp:localhost:14550"], TestAPMSim.dkBaud, 251, "DownloadWaypointTest")
        proxy.start()

        self.stressTestDownloadWP("udp:localhost:12052")

    def stressTestArm(self, connStr):
        for i in range(1, 5):
            vehicle = dronekit.connect(
                connStr,
                wait_ready=True,
                source_system=TestAPMSim.dkSSID,
                baud=TestAPMSim.dkBaud
            )

            VehicleFunctions.arm(vehicle)
            assert not vehicle.is_armable
            time.sleep(1)
            vehicle.armed = False
            vehicle.close()

    def test_canArm(self):
        sim, connStr = self.getSim()
        self.stressTestArm(connStr)

    def test_canArmThroughProxy(self):
        sim, connStr = self.getSim()
        proxy = Proxy(connStr, ["udp:localhost:12052", "udp:localhost:14550"], TestAPMSim.dkBaud, 251, "DownloadWaypointTest")
        proxy.start()

        self.stressTestArm("udp:localhost:12052")

class TestSolo(TestAPMSim):

    def getSim(self):
        return None, "udp:10.1.1.10:14550"