import unittest

from dronekit import LocationGlobalRelative

from pyspookystuff.mav.comm import Connection, Endpoint
from pyspookystuff.mav.sim import APMSim
from pyspookystuff_test.mav import move100m, numCores


# launch sim in same thread, control them from different thread
class NoProxy(unittest.TestCase):

    def setUp(self):
        self.sims = map(
            lambda i: APMSim(i),
            NoProxy.iis
        )

    def tearDown(self):
        self.sims = map(
            lambda sim: sim.close(),
            self.sims
        )

    endpoints = map(
        lambda i: Endpoint("tcp:localhost:" + str(5760 + i * 10)),
        iis
    )
    factory = None

    def test_1Connection(self):

        point = LocationGlobalRelative(0, 0, 20)
        conn = Connection.getOrCreate(
            self.endpoints,
            self.factory
        )

        vehicle = conn.vehicle

        move100m(point, vehicle)

        return vehicle.location.local_frame.north, vehicle.location.local_frame.east

    # def test_NConnection(self):
    #     self.assertEqual(True, False)


class WithProxy(NoProxy):
    pass