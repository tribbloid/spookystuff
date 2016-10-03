# Not part of the routing as its marginally useful outside testing.
import os

from dronekit import connect
from dronekit_sitl import SITL

from pyspookystuff import mav

# these are process-local and won't be shared by Spark workers

sitl_args = ['--model', 'quad', '--home=-35.363261,149.165230,584,353']

if 'SITL_SPEEDUP' in os.environ:
    sitl_args += ['--speedup', str(os.environ['SITL_SPEEDUP'])]
if 'SITL_RATE' in os.environ:
    sitl_args += ['-r', str(os.environ['SITL_RATE'])]


def tcp_master(instance):
    return 'tcp:127.0.0.1:' + str(5760 + instance*10)


class APMSim(object):
    existing = []
    usedINum = mav.manager.list()

    @staticmethod
    def nextINum():
        port = mav.nextUnused(APMSim.usedINum, range(0, 254))
        return port

    def __init__(self, index):

        self.iNum = index
        self.args = sitl_args + ['-I' + str(index)]
        self.sitl = SITL()
        self.sitl.download('copter', '3.3')
        self.sitl.launch(self.args, await_ready=True, restart=True)

        self.connStr = tcp_master(index)

        self.setParamAndRelaunch('SYSID_THISMAV', index + 1)

        APMSim.existing.append(self)

    @staticmethod
    def create():
        index = APMSim.nextINum()
        try:
            result = APMSim(index)
            return result
        except Exception as ee:
            APMSim.usedINum.remove(index)
            raise ee

    def setParamAndRelaunch(self, key, value):

        wd = self.sitl.wd
        v = connect(self.connStr, wait_ready=True)
        v.parameters.set(key, value, wait_ready=True)
        v.close()
        self.sitl.stop()
        self.sitl.launch(self.args, await_ready=True, restart=True, wd=wd, use_saved_data=True)
        v = connect(self.connStr, wait_ready=True)
        # This fn actually rate limits itself to every 2s.
        # Just retry with persistence to get our first param stream.
        v._master.param_fetch_all()
        v.wait_ready()
        actualValue = v._params_map[key]
        assert actualValue == value
        v.close()

    def close(self):

        self.sitl.stop()
        APMSim.usedINum.remove(self.iNum)
        APMSim.existing.remove(self)

    @staticmethod
    def clean():

        for sitl in APMSim.existing:
            sitl.stop()
        APMSim.existing = []
