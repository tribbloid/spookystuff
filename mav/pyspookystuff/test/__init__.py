from __future__ import print_function

import time
from nose import with_setup
from nose.tools import assert_equals

from pyspookystuff.mav import *


def setup_sitl_mavproxy(atype='quad', options=None, instance=0):
    setupSITL(instance)

    # time.sleep(2)
    # out = '127.0.0.1:' + str(14550 + instance*10)
    out = '127.0.0.1:' + str(14550)
    setupMAVProxy(atype=atype + str(instance), master=tcp_master(instance), outs={out}, options=options)


def teardownAll():
    teardownMAVProxy()
    teardownSITL()

def with_sitl(fn):

    @with_setup(setupSITL, teardownSITL)
    def test(*args, **kargs):
        tcp = fn('tcp:127.0.0.1:5760', *args, **kargs)
        teardownSITL()
        setup_sitl_mavproxy()
        udp = fn('127.0.0.1:14550', *args, **kargs)
        assert_equals(tcp, udp)
        return tcp

    test.__name__ = fn.__name__
    return test

def with_sitl_tcp(fn):

    @with_setup(setupSITL, teardownSITL)
    def test_tcp(*args, **kargs):
        tcp = fn('tcp:127.0.0.1:5760', *args, **kargs)
        return tcp

    test_tcp.__name__ = fn.__name__
    return test_tcp

def with_sitl_udp(fn):

    @with_setup(setup_sitl_mavproxy, teardownSITL)
    def test_udp(*args, **kargs):
        udp = fn('127.0.0.1:14550', *args, **kargs)
        return udp

    test_udp.__name__ = fn.__name__
    return test_udp

def with_sitl_3way(fn):

    @with_setup(setup_sitl_mavproxy(options='--out=127.0.0.1:10092'), teardownSITL)
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
