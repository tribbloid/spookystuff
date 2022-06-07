# Not part of the routing as its marginally useful outside testing.
from __future__ import print_function

from dronekit import connect
from dronekit_sitl import SITL

from pyspookystuff.uav.utils import retry
"""
crash course on APM 3.3 SITL
Options:
	--home HOME        set home location (lat,lng,alt,yaw)
	--model MODEL      set simulation model
	--wipe             wipe eeprom and dataflash
	--rate RATE        set SITL framerate
	--console          use console instead of TCP ports
	--instance N       set instance of SITL (adds 10*instance to all port numbers)
	--speedup SPEEDUP  set simulation speedup
	--gimbal           enable simulated MAVLink gimbal
	--autotest-dir DIR set directory for additional files
"""

# if 'SITL_SPEEDUP' in os.environ:
#     sitl_args += ['--speedup', str(os.environ['SITL_SPEEDUP'])]Proxy
# if 'SITL_RATE' in os.environ:
#     sitl_args += ['-r', str(os.environ['SITL_RATE'])]


def tcp_master(instance):
    return 'tcp:localhost:' + str(5760 + instance * 10)


class APMSim(object):

    def __init__(self, iNum, extraArgs, rate, speedup, vType="copter", version="3.3"):
        # type: (int, list[str], int, int, str, str) -> None
        """
        :param iNum: instance number, affect SITL system ID
        :param home: (lat,lng,alt,yaw)
        """
        self.iNum = iNum
        self.rate = rate
        self.args = extraArgs + ['-I' + str(iNum)] + ['--speedup', str(speedup)] + ['-r', str(rate)]
        self.vType = vType
        self.version = version

        sitl = SITL()
        self.sitl = sitl

        @retry(5)
        def download():
            sitl.download(vType, version)

        download()

        @retry(2)
        def launch():
            try:
                sitl.launch(self.args, await_ready=True, restart=True)
                print("launching APM SITL:", *(self.args + ["URI=" + self.connStr] + ["PID=" + str(sitl.p.pid)]))
                self.setParamAndRelaunch('SYSID_THISMAV', self.iNum + 1)

                @self.withVehicle()
                def set(vehicle):
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
                v = connect(self.connStr, wait_ready=True, baud=self.rate)
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
