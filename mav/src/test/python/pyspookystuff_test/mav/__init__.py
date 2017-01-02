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

    @classmethod
    def setUpClass(cls):
        cls.dkSSID = 250
        cls.dkBaud = 57600
        cls.sim = None

    def tearDown(self):
        self.sim.close()
        self.sim = None

    def stressTestDownloadWP(self, connStr):
        for i in range(1, 20):
            print("stress test:", i, "time(s)")
            vehicle = dronekit.connect(
                connStr,
                wait_ready=True,
                source_system=self.dkSSID,
                baud=self.dkBaud
            )


            try:
                @retry(2)
                def blockingDownload():
                    vehicle.commands.download()
                    vehicle.commands.wait_ready()
                blockingDownload()
            finally:
                vehicle.close()

    @property
    def url(self):
        if not self.sim:
            self.sim = APMSim(0, "43.694195,-79.262262,136,353", self.dkBaud)
        return self.sim.connStr

    @property
    def gcs(self):
        return "udp:localhost:14560"

    def test_downloadWaypointCannotTimeout(self):
        connStr = self.url
        self.stressTestDownloadWP(connStr)

    def test_downloadWaypointThroughProxyCannotTimeout(self):
        connStr = self.url
        proxy = Proxy(connStr, ["udp:localhost:12052", self.gcs], self.dkBaud, 251, "DownloadWaypointTest")
        proxy.start()

        self.stressTestDownloadWP("udp:localhost:12052")

    def stressTestArm(self, connStr):
        for i in range(1, 5):
            print("stress test:", i, "time(s)")
            vehicle = dronekit.connect(
                connStr,
                wait_ready=True,
                source_system=self.dkSSID,
                baud=self.dkBaud
            )
            try:
                VehicleFunctions.arm(vehicle, "ALT_HOLD", False)  # No GPS
                assert(vehicle.armed is True)
                VehicleFunctions.unarm(vehicle)
                time.sleep(1)
                assert(vehicle.armed is False)
            finally:
                vehicle.close()

    def test_canArm(self):
        connStr = self.url
        self.stressTestArm(connStr)

    def test_canArmThroughProxy(self):
        connStr = self.url
        proxy = Proxy(connStr, ["udp:localhost:12052", self.gcs], self.dkBaud, 251, "DownloadWaypointTest")
        proxy.start()

        self.stressTestArm("udp:localhost:12052")

class TestSolo(TestAPMSim):

    @property
    def url(self):
        return "udpin:0.0.0.0:14550"