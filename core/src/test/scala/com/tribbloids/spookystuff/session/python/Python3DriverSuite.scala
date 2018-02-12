package com.tribbloids.spookystuff.session.python

import com.tribbloids.spookystuff.session.python.Python2DriverSuite.Runner

class Python3DriverSuite extends Python2DriverSuite {

  override lazy val runner: Runner = Python2DriverSuite.Runner3
}
