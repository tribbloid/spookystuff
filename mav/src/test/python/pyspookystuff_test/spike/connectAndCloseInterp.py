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
    [ "udp:localhost:12015" ]
    """
))
link4395604505930358814=pyspookystuff.mav.telemetry.Link(endpoint=endpoint3641353257086284776, proxyOpt=None)

link4395604505930358814.testMove()

# link4395604505930358814.testMove()

link4395604505930358814.stop()

# time.sleep(1000)
