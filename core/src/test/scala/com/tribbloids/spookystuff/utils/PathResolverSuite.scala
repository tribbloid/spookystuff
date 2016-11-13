package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture
import org.apache.hadoop.security.UserGroupInformation

/**
  * Created by peng on 01/09/16.
  */
class PathResolverSuite extends SpookyEnvFixture with LocalPathDocsFixture {

  val resolver = HDFSResolver(sc.hadoopConfiguration, Some(UserGroupInformation.createUserForTesting("dummy",Array.empty)))

  test("HDFSResolver can override login UGI") {
    val user = resolver.input(HTML_URL) {
      is =>
        UserGroupInformation.getCurrentUser.getUserName
    }
    user.shouldBe("dummy")
  }

  test("HDFSResolver can override login GUI on executors") {
    val resolver = this.resolver
    val HTML_URL = this.HTML_URL
    val users = sc.parallelize(1 to (sc.defaultParallelism * 2))
      .mapPartitions {
        itr =>
          val str = resolver.input(HTML_URL) {
            is =>
              UserGroupInformation.getCurrentUser.getUserName
          }
          Iterator(str)
      }
      .collect()
      .distinct
      .mkString("\n")
    users.shouldBe("dummy")
  }
}
