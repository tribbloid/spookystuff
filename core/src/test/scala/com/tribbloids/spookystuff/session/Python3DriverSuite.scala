package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.session.Python2DriverSuite.Runner

class Python3DriverSuite extends Python2DriverSuite {

  override lazy val runner: Runner = Python2DriverSuite.Runner3
}
