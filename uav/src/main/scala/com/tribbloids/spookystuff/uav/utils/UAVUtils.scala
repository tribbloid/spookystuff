package com.tribbloids.spookystuff.uav.utils

import com.tribbloids.spookystuff.python.ref.PyRef
import com.tribbloids.spookystuff.session.ConflictDetection
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.uav.telemetry.mavlink.MAVLink
import com.tribbloids.spookystuff.utils.TreeException
import org.apache.spark.SparkContext

import scala.util.Try

object UAVUtils {

  def localSanityTrials: Seq[Try[Unit]] = {
    Seq(
      Try(PyRef.sanityCheck()),
      Try(Link.sanityCheck()),
      Try(MAVLink.sanityCheck())
    ) ++
      ConflictDetection.conflicts
  }

  def localSanityCheck = TreeException.&&&(localSanityTrials)

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def sanityCheck(sc: SparkContext) = {
    sc.runEverywhere()(
      _ => localSanityCheck
    )
  }
}
