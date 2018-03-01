# tests in this package are only tested locally but not on CI server, they should be lightweight.

from __future__ import print_function

import os
from unittest import TestCase
from unittest import skip

import dronekit

from pyspookystuff.uav.sim import APMSim
from pyspookystuff.uav.telemetry.mavlink import Proxy
from pyspookystuff.uav.utils import retry

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

class TestProxy(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dkSSID = 250
        cls.dkBaud = 57600
        cls.sim = None

    @property
    def gcs(self):
        return "udp:localhost:14560"

    @property
    def url(self):
        if not self.sim:
            self.sim = APMSim(0, ["--home", "43.694195,-79.262262,136,353"], 200, 5)
        return self.sim.connStr

    def testProxyRestart(self):

        proxy = Proxy(self.url, ["udp:localhost:12052", self.gcs], self.dkBaud, 251, self.__class__.__name__)

        for i in range(1, 3):
            proxy.start()
            proxy.stop()

    def testProxyToNonExistingDrone(self):
        proxy = Proxy("udp:dummy:1000", ["udp:localhost:12052", self.gcs], self.dkBaud, 251, self.__class__.__name__)

        try:
            proxy.start()
        except Exception as e:
            print(e)
            return
        else:
            raise os.error("IMPOSSIBLE!")


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
            self.sim = APMSim(0, ["--home", "43.694195,-79.262262,136,353"], 200, 5)
        return self.sim.connStr

    @property
    def gcs(self):
        return "udp:localhost:14560"

    def test_downloadWaypointCannotTimeout(self):
        connStr = self.url
        self.stressTestDownloadWP(connStr)

    def test_downloadWaypointThroughProxyCannotTimeout(self):
        connStr = self.url
        proxy = Proxy(connStr, ["udp:localhost:12052", self.gcs], self.dkBaud, 251, self.__class__.__name__)
        proxy.start()

        self.stressTestDownloadWP("udp:localhost:12052")

    def stressTestArm(self, connStr):
        pass
        # for i in range(1, 5):
        #     print("stress test:", i, "time(s)")
        #     vehicle = dronekit.connect(
        #         connStr,
        #         wait_ready=True,
        #         source_system=self.dkSSID,
        #         baud=self.dkBaud
        #     )
        #     try:
        #         vf = VehicleFunctions(vehicle)
        #         vf.arm( "ALT_HOLD", False)  # No GPS
        #         assert(vehicle.armed is True)
        #         vf.unarm()
        #         time.sleep(1)
        #         assert(vehicle.armed is False)
        #     finally:
        #         vehicle.close()

    def test_canArm(self):
        connStr = self.url
        self.stressTestArm(connStr)

    def test_canArmThroughProxy(self):
        connStr = self.url
        proxy = Proxy(connStr, ["udp:localhost:12052", self.gcs], self.dkBaud, 251, self.__class__.__name__)
        proxy.start()

        self.stressTestArm("udp:localhost:12052")

@skip("connect to solo first")
class TestSolo(TestAPMSim):

    @property
    def url(self):
        return "udpin:0.0.0.0:14550"