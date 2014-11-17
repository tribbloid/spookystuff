package org.tribbloid.spookystuff.example.price.cloud

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore

/**
 * Created by peng on 24/08/14.
 */
object Rackspace extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._
    noInput
      .fetch(
        Wget("http://www.rackspace.com/cloud/servers/")
      )
      .sliceJoin("tr.pricing-row")(indexKey = 'row)
      .extract(
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
      )
      .asSchemaRDD()
  }
}
