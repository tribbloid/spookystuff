from __future__ import print_function

import time
from nose import with_setup

from pyspookystuff.mav.routing import Proxy
from pyspookystuff.mav.sim import APMSim


def sitlUp():
    APMSim.create()


def sitlClean():
    APMSim.clean()


def sitlProxyUp(atype='quad', outs=list()):
    sim = APMSim.create()
    Proxy(sim.connStr, atype + str(sim.index), 14550, outs)


def sitlProxyClean():
    Proxy.clean()
    APMSim.clean()


def with_sitl_tcp(fn):

    @with_setup(sitlUp, sitlClean)
    def test_tcp(*args, **kargs):
        tcp = fn('tcp:127.0.0.1:5760', *args, **kargs)
        return tcp

    test_tcp.__name__ = fn.__name__
    return test_tcp


def with_sitl_udp(fn):

    @with_setup(sitlProxyUp, sitlProxyClean)
    def test_udp(*args, **kargs):
        udp = fn('127.0.0.1:14550', *args, **kargs)
        return udp

    test_udp.__name__ = fn.__name__
    return test_udp


def with_sitl_3way(fn):

    @with_setup(sitlProxyUp(outs=['127.0.0.1:10092']), sitlClean)
    def test_udp(*args, **kargs):
        udp = fn('127.0.0.1:10092', *args, **kargs)
        return udp

    test_udp.__name__ = fn.__name__
    return test_udp


def wait_for(condition, time_max):
    time_start = time.time()
    while not condition():
        if time.time() - time_start > time_max:
            break
        time.sleep(0.1)
