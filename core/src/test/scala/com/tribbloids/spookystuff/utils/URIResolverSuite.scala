package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture
import com.tribbloids.spookystuff.utils.io.HDFSResolver
import org.apache.hadoop.security.UserGroupInformation

/**
  * Created by peng on 01/09/16.
  */
class URIResolverSuite extends SpookyEnvFixture with LocalPathDocsFixture {

  val resolverWithUser = HDFSResolver(
    sc.hadoopConfiguration,
    () => Some(UserGroupInformation.createUserForTesting("dummy", Array.empty))
  )

  it("HDFSResolver can override login UGI") {
    val user: String = resolverWithUser.input(HTML_URL) {
      is =>
        UserGroupInformation.getCurrentUser.getUserName
    }
    user.shouldBe("dummy")
  }

  it("HDFSResolver can override login GUI on executors") {
    val resolver = this.resolverWithUser
    val HTML_URL = this.HTML_URL
    val users = sc.parallelize(1 to (sc.defaultParallelism * 2))
      .mapPartitions {
        itr =>
          val str: String = resolver.input(HTML_URL) {
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
