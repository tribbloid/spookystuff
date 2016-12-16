### DON'T DELETE, FIXTURES!

import sys
import time

sys.path.append('/home/peng/.spookystuff/pythonpath')
import os
import simplejson as json

import pyspookystuff.mav.sim
import pyspookystuff.mav.telemetry

aPMSim60043357380016504=pyspookystuff.mav.sim.APMSim(
    iNum=json.loads(
        """
        0
        """
    )
)

proxy=pyspookystuff.mav.telemetry.Proxy(
    master="tcp:localhost:5760",
    name="DRONE",
    outs = ["udp:localhost:12015", "udp:localhost:14550"]
)

proxy.start()


try:
    endpoint1703625946072222920=pyspookystuff.mav.telemetry.Endpoint(connStrs=json.loads(
        """
        [ "tcp:localhost:5760" ]
        """
    ))
    link6964116197604848232=pyspookystuff.mav.telemetry.Link(endpoint=endpoint1703625946072222920, outs=json.loads(
        """
        [ "udp:localhost:12015", "udp:localhost:14550" ]
        """
    ), name=json.loads(
        """
        "DRONE"
        """
    ))
    start6788580466239706406=link6964116197604848232.start()
except Exception as e:
    print('======== *!?error info!?* ========')
    raise

try:
    testMove8942317931611326564=link6964116197604848232.testMove()
except Exception as e:
    print('======== *!?error info!?* ========')
    raise