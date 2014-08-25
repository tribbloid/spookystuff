package org.tribbloid.spookystuff.acceptance.cloud

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.acceptance.SparkTestCore
import org.tribbloid.spookystuff.entity._

/**
 * Created by peng on 24/08/14.
 */
object Rackspace extends SparkTestCore {

  override def doMain(): Array[_] = {
    (sc.parallelize(Seq(null))+>
      Wget("http://www.rackspace.com/cloud/servers/") !==)
      .leftJoinBySlice(
        "tr.pricing-row"
      ).selectInto(
        "type" -> {_.attr1("tr","class").replaceAll("pricing-row","").replaceAll("-p1|-p2|-dark","")},
        "name" -> {_.text("td")(0)},
        "RAM" -> {_.text("td")(1)},
        "vCPUs" -> {_.text("td")(2)},
        "System_Disk" -> {_.text("td")(3)},
        "Data_Disk" -> {_.text("td")(4)},
        "Bandwidth" -> {_.text("td")(5)},
        "Raw_Infrastructure" -> {_.text("td")(6)},
        "Managed_Infrastructure" -> {_.text("td")(8)},
        "Managed_Operations_SysOps" -> {_.text("td")(9)},
        "Managed_Operations_DevOps_Automation" -> {_.text("td")(10)}
      ).asJsonRDD
      .collect()
  }
}
