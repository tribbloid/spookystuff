### DON'T DELETE, FIXTURES!

from __future__ import print_function

import time

import pyspookystuff.uav.telemetry
proxy1=pyspookystuff.uav.telemetry.Proxy(
    master="tcp:localhost:5770",
    name="DRONE",
    outs = ["udp:localhost:12015", "udp:localhost:14550"]
)
proxy1.startAndBlock()

proxy2=pyspookystuff.uav.telemetry.Proxy(
    master="tcp:localhost:5780",
    name="DRONE",
    outs = ["udp:localhost:12016", "udp:localhost:14550"]
)
proxy2.startAndBlock()

while True:
    print("zzzzzzzzzzzzzzzzzz...")
    time.sleep(10)