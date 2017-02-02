### DON'T DELETE, FIXTURES!

# sys.path.append('/home/peng/.spookystuff/pythonpath')
# import os
# import simplejson as json
#
# import pyspookystuff.mav.sim
# aPMSim60043357380016504=pyspookystuff.mav.sim.APMSim(
#     iNum=json.loads(
#         """
#         4
#         """
#     )
# )

# Error interpreting:
import simplejson as json

import pyspookystuff.mav.telemetry

endpoint3641353257086284776=pyspookystuff.mav.telemetry.Endpoint(connStrs=json.loads(
    """
    [ "tcp:localhost:5800" ]
    """
))
proxy8080968396647159412=pyspookystuff.mav.telemetry.Proxy(master=json.loads(
    """
    "tcp:localhost:5800"
    """
), outs=json.loads(
    """
    [ "udp:localhost:12015", "udp:localhost:14550" ]
    """
), name=json.loads(
    """
    "DRONE@tcp:localhost:5800"
    """
))
link4395604505930358814=pyspookystuff.mav.telemetry.Link(endpoint=endpoint3641353257086284776, proxyOpt=proxy8080968396647159412)
proxyPID511134141563190063=link4395604505930358814.proxyPID

link4395604505930358814.testMove()

# link4395604505930358814.testMove()

link4395604505930358814.stop()

# time.sleep(1000)
