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
    endpoint6689066920223517022=pyspookystuff.mav.telemetry.Endpoint(connStrs=json.loads(
        """
        [ "tcp:localhost:5800" ]
        """
    ), name=json.loads(
        """
        "DRONE"
        """
    ))
    link4278034394758385026=pyspookystuff.mav.telemetry.Link(endpoint=endpoint6689066920223517022, outs=json.loads(
        """
        [ "udp:localhost:12068", "udp:localhost:14550" ]
        """
    ))
    link1615522973475451768=pyspookystuff.mav.telemetry.Link(endpoint=endpoint6689066920223517022, outs=json.loads(
        """
        [ "udp:localhost:12033", "udp:localhost:14550" ]
        """
    ))
    start9057863921842577795=link1615522973475451768.start()
except Exception as e:
    print('======== *!?error info!?* ========')
    raise

try:
    testMove8942317931611326564=link6964116197604848232.testMove()
except Exception as e:
    print('======== *!?error info!?* ========')
    raise