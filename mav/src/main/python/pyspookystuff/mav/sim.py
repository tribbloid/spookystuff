# Not part of the routing as its marginally useful outside testing.
from __future__ import print_function

import os

from dronekit import connect
from dronekit_sitl import SITL

from pyspookystuff.mav.utils import retry

sitl_args = ['--model', 'quad', '--home=-35.363261,149.165230,584,353']

if 'SITL_SPEEDUP' in os.environ:
    sitl_args += ['--speedup', str(os.environ['SITL_SPEEDUP'])]
if 'SITL_RATE' in os.environ:
    sitl_args += ['-r', str(os.environ['SITL_RATE'])]


def tcp_master(instance):
    return 'tcp:localhost:' + str(5760 + instance*10)

class APMSim(object):

    def __init__(self, iNum):
        # type: (int) -> None

        self.iNum = iNum
        self.args = sitl_args + ['-I' + str(iNum)]
        sitl = SITL()
        self.sitl = sitl

        @retry(5)
        def download():
            sitl.download('copter', '3.3')
        download()

        @retry(5)
        def launch():
            try:
                sitl.launch(self.args, await_ready=True, restart=True)
                print("launching APM SITL ... PID =", str(sitl.p.pid))
                self.setParamAndRelaunch('SYSID_THISMAV', self.iNum + 1)

                @self.withVehicle()
                def set(vehicle):
                    # TODO: spookystuff should not set any parameter! move to SITL.
                    vehicle.parameters['FS_GCS_ENABLE'] = 0
                    vehicle.parameters['FS_EKF_THRESH'] = 100
                set()

            except:
                self.close()
                raise

        launch()
        print("APM SITL on", self.connStr, "is up and running")

    def _getConnStr(self):
        return tcp_master(self.iNum)

    @property
    def connStr(self):
        return self._getConnStr()

    def setParamAndRelaunch(self, key, value):
        # type: (str, int) -> None

        wd = self.sitl.wd  # path of the eeprom file

        @self.withVehicle()
        def set(v):
            v.parameters.set(key, value, wait_ready=True)
        set()

        self.sitl.stop()
        self.sitl.launch(self.args, await_ready=True, restart=True, wd=wd, use_saved_data=True)

        @self.withVehicle()
        def get(v):
            v._master.param_fetch_all()
            v.wait_ready()
            actualValue = v._params_map[key]
            assert actualValue == value
        get()

    def withVehicle(self):
        def decorate(fn):
            def fnM(*args, **kargs):
                v = connect(self.connStr, wait_ready=True)
                try:
                    return fn(v, *args, **kargs)
                finally:
                    v.close()
            return fnM
        return decorate

    def close(self):
        if self.sitl:
            try:
                self.sitl.stop()
                print("Cleaned up APM SITL PID =", str(self.sitl.p.pid))
            except:
                pass
        else:
            # print("APM SITL not initialized, do not clean")
            pass

    def __del__(self):
        self.close()
