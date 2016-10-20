from __future__ import print_function

import pickle
import unittest

import time
from dronekit import connect

from pyspookystuff.mav.comm import ProxyFactory
from pyspookystuff.mav.sim import APMSim, usedINums
from pyspookystuff_test.mav import moveOut, numCores, APMSimFixture, APMSimContext, AbstractIT


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

class Suite(APMSimFixture):

    @staticmethod
    def test_canBePickled():
        sim = getSim(0)
        print(pickle.dumps(sim))

    def test_nextINumIsProcessSafe(self):
        iNums = self.processPool.map(
            getINum,
            range(0, numCores)
        )

        assert sorted(iNums) == range(0, numCores), iNums

    def test_createIsProcessSafe(self):
        iNums = self.processPool.map(
            getSim,
            range(0, numCores)
        )

        assert sorted(iNums) == range(0, numCores), iNums

    @staticmethod
    def test_randomLocations():
        result = Suite.randomLocations()
        coordinate = map(
            lambda v: (v.lat, v.lon),
            result
        )
        print(coordinate)
        assert(len(set(coordinate)) != len(coordinate), (len(set(coordinate)), len(coordinate)))

defaultProxyFactory = ProxyFactory()

def _move(point, proxyFactory=None):

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

            moveOut(point, vehicle)

        finally:
            if proxy:
                proxy.close()

        return vehicle.location.local_frame.north, vehicle.location.local_frame.east

def move_NoProxy(tuple):
    return _move(tuple)
def move_Proxy(tuple):
    return _move(tuple, defaultProxyFactory)
class SimpleMoveIT(AbstractIT):

    @staticmethod
    def getFns():
        return [move_NoProxy, move_Proxy]

if __name__ == '__main__':
    unittest.main()