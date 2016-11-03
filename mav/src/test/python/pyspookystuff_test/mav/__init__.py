from __future__ import print_function

import multiprocessing
import multiprocessing.pool
import random
from unittest import TestCase

import time
from dronekit import LocationGlobalRelative
from math import sqrt

from pyspookystuff.mav import assureInTheAir
from pyspookystuff.mav.comm import Proxy, Endpoint
from pyspookystuff.mav.sim import APMSim


def sitlProxyUp(atype='quad', outs=list()):
    sim = APMSim.create()
    Proxy(sim._getConnStr, atype + str(sim.index), 14550, outs)


numCores = multiprocessing.cpu_count()


def moveOut(point, vehicle):



class APMSimContext(object):

    def __init__(self):
        pass

    def __enter__(self):
        sim = APMSim.create()
        self.sim = sim
        return sim

    def __exit__(self, type, value, tb):
        if tb:
            print("### ERROR ### [> ", str(type), ":", str(value))
            return False

        self.sim.close()
        return True

endpoints = map(
    lambda i: Endpoint("tcp:localhost:" + str(5760 + i * 10)),
    range(0, numCores)
)

# always launch 1 APM per core
# remember to initialize sim.connStr in each process but not in single thread or multi thread iterations, both are slow!
class APMSimFixture(TestCase):

    def setUp(self):
        # use thread pool instead of process pool to avoid
        self.threadPool = multiprocessing.pool.ThreadPool()
        self.processPool = multiprocessing.Pool()

    def tearDown(self):
        pass

    # def test_empty(self):
    #     pass

    @staticmethod
    def randomLocation():
        return LocationGlobalRelative(random.uniform(-90, 90), random.uniform(-180, 180), 20)

    @staticmethod
    def randomLocations():
        points = map(
            lambda i: APMSimFixture.randomLocation(),
            range(0, numCores)
        )
        return points


class ParallelCase(APMSimFixture):

    @staticmethod
    def testFunctions():
        return []

    def test_move1(self):
        fns = self.testFunctions()
        for fn in fns:
            print("### test_move1: SUBTEST ### [> ", fn.__name__)
            result = fn(0)
            print(result)

    # def test_moveN(self):
    #     fns = self.getFns()
    #     for fn in fns:
    #         print("### test_moveN: SUBTEST ### [> ", fn.__name__)
    #         positions = self.processPool.map(
    #             fn,
    #             self.randomLocations()
    #         )
    #         print(positions)


    #         assert len(set(positions)) == len(positions)

class AbstractIT(ParallelCase, APMSimFixture):
    pass