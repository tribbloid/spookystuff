import time
from dronekit import connect, LocationGlobalRelative

from pyspookystuff.mav import assureInTheAir
from pyspookystuff.mav.routing import Proxy
from pyspookystuff.mav.sim import APMSim


# should test 1 drone, 2 drones, 2 drones on different processes.
def simMove_Proxy(point, proxyOut=None):
    #always move 100m north.

    sim = APMSim.create()

    endpoint = sim.connStr
    proxy = None
    if proxyOut:
        proxy = Proxy(sim.connStr, "quad" + str(sim.iNum), 14550, [proxyOut])
        endpoint = proxyOut

    vehicle = connect(endpoint, wait_ready=True)

    # NOTE these are *very inappropriate settings*
    # to make on a real vehicle. They are leveraged
    # exclusively for simulation. Take heed!!!
    vehicle.parameters['FS_GCS_ENABLE'] = 0
    vehicle.parameters['FS_EKF_THRESH'] = 100

    print("Allowing time for parameter write")

    assureInTheAir(20, vehicle)

    print("Going to point...")
    vehicle.simple_goto(point)

    def distSq():
        north = vehicle.location.local_frame.north
        east = vehicle.location.local_frame.east
        return north*north + east*east

    while distSq() <= 10000: # close enough
        print("moving north ... ", vehicle.location.local_frame.north, "m")
        time.sleep(1)

    if proxy:
        proxy.close()

    sim.close()


def simMove_ProxyFac(fac=None):
    pass


def test1drone():
    simMove_Proxy(LocationGlobalRelative(-34.363261, 149.165230, 20))

def test1drone_proxy():
    simMove_Proxy(LocationGlobalRelative(-34.363261, 149.165230, 20), 'localhost:10092')
