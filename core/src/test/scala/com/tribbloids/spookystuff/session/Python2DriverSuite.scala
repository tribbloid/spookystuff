package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.session.Python3DriverSuite.Runner

class Python2DriverSuite extends Python3DriverSuite {

  override lazy val runner: Runner = Python3DriverSuite.Runner2
}
