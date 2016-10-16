from __future__ import print_function

import pickle
import traceback

import time
from dronekit import connect, LocationGlobalRelative

from pyspookystuff.mav.comm import ProxyFactory
from pyspookystuff.mav.sim import APMSim, usedINums
from pyspookystuff_test.mav import move100m, numCores, APMSimFixture, randomLocations, APMSimContext


def getINum(i):
    iNum = APMSim.nextINum()
    time.sleep(5)
    usedINums.remove(iNum)
    return iNum


def getSim(i):
    sim = APMSim.create()
    try:
        iNum = sim.iNum
        time.sleep(5)
        return iNum
    finally:
        sim.close()

class ProcessSafety(APMSimFixture):

    @staticmethod
    def test_canBePickled():
        sim = getSim(0)
        print(pickle.dumps(sim))

    def test_nextINum(self):
        iNums = self.processPool.map(
            getINum,
            range(0, numCores)
        )

        assert sorted(iNums) == range(0, numCores), iNums

    def test_APMSim_create(self):
        iNums = self.processPool.map(
            getSim,
            range(0, numCores)
        )

        assert sorted(iNums) == range(0, numCores), iNums

    def test_randomLocations(self):
        result = randomLocations()
        coordinate = map(
            lambda v: (v.lat, v.lon),
            result
        )
        print(coordinate)
        assert(len(set(coordinate)) != len(coordinate), (len(set(coordinate)), len(coordinate)))

defaultProxyFactory = ProxyFactory()

def _move(point, proxyFactory=None):
    try:
        with APMSimContext() as sim:
            # type: (LocationGlobal, ProxyFactory) -> double, double
            # always move 100m.g

            # sim = APMSim.create()
            uri = sim.connStr
            print("Connecting to ... ", uri)

            proxy = None
            try:

                if proxyFactory:
                    proxy = proxyFactory.nextProxy(uri)
                    uri = proxy.uri

                vehicle = connect(uri, wait_ready=True)

                move100m(point, vehicle)

            finally:
                if proxy:
                    proxy.close()

            return vehicle.location.local_frame.north, vehicle.location.local_frame.east
    except:
        traceback.print_exc()
        raise

def simMove(tuple):
        return _move(tuple)
class NoProxy(APMSimFixture):

    def __init__(self, *args, **kwargs):
        super(NoProxy, self).__init__(*args, **kwargs)
        self.simMove = simMove

    def test_move1Drone(self):
        position = self.simMove(LocationGlobalRelative(-34.363261, 149.165230, 20))
        print(position)

    def test_moveNDrones(self):

        positions = self.processPool.map(
            self.simMove,
            randomLocations()
        )
        print(positions)
        assert len(set(positions)) == len(positions)


def simMoveProxy(tuple):
        return _move(tuple, defaultProxyFactory)
class WithProxy(NoProxy):

    def __init__(self, *args, **kwargs):
        super(WithProxy, self).__init__(*args, **kwargs)
        self.simMove = simMoveProxy

        # simMove = simMoveProxy

        # def simMove(self, p):
        #     global defaultProxyFactory
        #
        #     _simMove(p, defaultProxyFactory)
