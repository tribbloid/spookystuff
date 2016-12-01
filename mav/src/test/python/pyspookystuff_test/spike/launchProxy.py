### DON'T DELETE, FIXTURES!

from __future__ import print_function

import time

import pyspookystuff.mav.telemetry
proxy=pyspookystuff.mav.telemetry.Proxy(
    master="tcp:localhost:5800",
    name="DRONE",
    outs = ["udp:localhost:12015", "udp:localhost:14550"]
)

proxy.start()

while True:
    print("zzzzzzzzzzzzzzzzzz...")
    time.sleep(10)