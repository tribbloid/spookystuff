import multiprocessing
import random
from unittest import TestCase

import time
from dronekit import connect, LocationGlobalRelative

from pyspookystuff.mav import assureInTheAir
from pyspookystuff.mav.routing import ProxyFactory
from pyspookystuff.mav.sim import APMSim


# should move 1 drone, 2 drones on different multiprocessing.Processes.
def _simMove(point, proxyFactory=None):
    # type: (LocationGlobal, ProxyFactory) -> double, double
    # always move 100m.

    sim = APMSim.create()
    endpoint = sim.connStr

    proxy = None
    if proxyFactory:
        proxy = proxyFactory.nextProxy(sim.connStr)
        endpoint = proxy.endpoint

    vehicle = connect(endpoint, wait_ready=True)

    # NOTE these are *very inappropriate settings*
    # to make on a real vehicle. They are leveraged
    # exclusively for simulation. Take heed!!!
    vehicle.parameters['FS_GCS_ENABLE'] = 0
    vehicle.parameters['FS_EKF_THRESH'] = 100

    print("Allowing time for parameter write")

    assureInTheAir(20, vehicle)

    print("Going to point...")
    vehicle.simple_goto(point)

    def distSq():
        north = vehicle.location.local_frame.north
        east = vehicle.location.local_frame.east
        return north * north + east * east

    while distSq() <= 10000:  # 100m
        print("moving ... " + str(vehicle.location.local_frame.north) + "m")
        time.sleep(1)

    if proxy:
        proxy.close()

    sim.close()
    return vehicle.location.local_frame.north, vehicle.location.local_frame.east


def simMove(p):
    # type: (object) -> object
    return _simMove(p)


def simMoveProxy(p):
    return _simMove(p, defaultProxyFactory)


def nextINum(i):
    iNum = APMSim.nextINum()
    time.sleep(5)
    APMSim.usedINums.remove(iNum)
    return iNum


def nextSim(i):
    sim = APMSim.create()
    # try:
    iNum = sim.iNum
    time.sleep(5)
    # finally: TODO: revert
    sim.close()
    return iNum


numCores = multiprocessing.cpu_count()


class ProcessSafety(TestCase):
    global numCores

    def __init__(self, *args, **kwargs):
        self.pool = multiprocessing.Pool()
        super(ProcessSafety, self).__init__(*args, **kwargs)

    def test_nextINum(self):
        iNums = self.pool.map(
            nextINum,
            range(0, numCores)
        )

        assert sorted(iNums) == range(0, numCores), iNums

    def test_APMSim_create(self):
        iNums = self.pool.map(
            nextSim,
            range(0, numCores)
        )

        assert sorted(iNums) == range(0, numCores), iNums


defaultProxyFactory = ProxyFactory()


class NoProxy(TestCase):
    global numCores

    def __init__(self, *args, **kwargs):
        self.pool = multiprocessing.Pool()
        super(NoProxy, self).__init__(*args, **kwargs)
        self.simMove = simMove

    def test_move1drone(self):
        position = self.simMove(LocationGlobalRelative(-34.363261, 149.165230, 20))
        print(position)

    def test_moveNdrones(self):
        points = map(
            lambda i: LocationGlobalRelative(random.uniform(-90, 90), random.uniform(-180, 180), 20),
            range(0, numCores)
        )

        positions = self.pool.map(
            self.simMove,
            points
        )
        print(positions)
        assert len(set(positions)) == len(positions)


class WithProxy(NoProxy):

    def __init__(self, *args, **kwargs):
        self.pool = multiprocessing.Pool()
        super(NoProxy, self).__init__(*args, **kwargs)
        self.simMove = simMoveProxy

        # simMove = simMoveProxy

        # def simMove(self, p):
        #     global defaultProxyFactory
        #
        #     _simMove(p, defaultProxyFactory)
