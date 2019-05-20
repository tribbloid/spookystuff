### DON'T DELETE, FIXTURES!

from __future__ import print_function

import time

from pyspookystuff.uav.telemetry.mavlink import MAVProxy

proxy = MAVProxy(master="tcp:localhost:5800", name="DRONE", outs=["udp:localhost:12015", "udp:localhost:14550"])

proxy.startAndBlock()

while True:
    print("zzzzzzzzzzzzzzzzzz...")
    time.sleep(10)
