from __future__ import print_function

import multiprocessing
import multiprocessing.pool
import random
import traceback
from unittest import TestCase

import time
from dronekit import LocationGlobalRelative
from math import sqrt

from pyspookystuff.mav import assureInTheAir
from pyspookystuff.mav.comm import Proxy
from pyspookystuff.mav.sim import APMSim


def sitlUp():
    APMSim.create()


def sitlProxyUp(atype='quad', outs=list()):
    sim = APMSim.create()
    Proxy(sim._getConnStr, atype + str(sim.index), 14550, outs)


def wait_for(condition, time_max):
    time_start = time.time()
    while not condition():
        if time.time() - time_start > time_max:
            break
        time.sleep(0.1)


numCores = multiprocessing.cpu_count()


def move100m(point, vehicle):
    # NOTE these are *very inappropriate settings*
    # to make on a real vehicle. They are leveraged
    # exclusively for simulation. Take heed!!!
    vehicle.parameters['FS_GCS_ENABLE'] = 0
    vehicle.parameters['FS_EKF_THRESH'] = 100
    print("Allowing time for parameter write")
    assureInTheAir(20, vehicle)
    print("Going to point...")
    vehicle.simple_goto(point)

    def getDistSq():
        north = vehicle.location.local_frame.north
        east = vehicle.location.local_frame.east
        return north * north + east * east

    distSq = getDistSq()
    while distSq <= 10000:  # 100m
        print("moving ... " + str(sqrt(distSq)) + "m")
        distSq = getDistSq()
        time.sleep(1)

def randomLocation():
    return LocationGlobalRelative(random.uniform(-90, 90), random.uniform(-180, 180), 20)

def randomLocations():
    points = map(
        lambda i: randomLocation(),
        range(0, numCores)
    )
    return points

def APMSimFactories(i):
    def factory():
        try:
            return APMSim.create()
        except:
            traceback.print_exc()
            raise
    return factory


def simDown(sim):
    sim.close()


class APMSimContext(object):

    def __init__(self):
        pass

    def __enter__(self):
        sim = APMSim.create()
        self.sim = sim
        return sim

    def __exit__(self, type, value, traceback):
        self.sim.close()


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
