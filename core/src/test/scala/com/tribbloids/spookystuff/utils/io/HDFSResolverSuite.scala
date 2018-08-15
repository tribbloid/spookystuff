package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

/**
 * Created by peng on 07/10/15.
 */
class HDFSResolverSuite extends LocalResolverSuite {

  @transient override lazy val resolver = HDFSResolver(new Configuration())
  @transient override lazy val schemaPrefix = "file:"

  val nonExistingSchemePath = "file:/non-existing/not-a-file.txt"
  val nonExistingScheme2Path = "file:///non-existing/not-a-file.txt"

  it("can convert path with schema of non-existing file") {
    val abs = resolver.toAbsolute(nonExistingSchemePath)
    assert(abs == nonExistingSchemePath)
  }

  it("can convert path with schema// of non-existing file") {
    val abs = resolver.toAbsolute(nonExistingScheme2Path)
    assert(abs == nonExistingSchemePath)
  }

  val resolverWithUser = HDFSResolver(
    new Configuration(),
    () => Some(UserGroupInformation.createUserForTesting("dummy", Array.empty))
  )

  it("can override login UGI") {
    val user: String = resolverWithUser.input(HTML_URL) {
      is =>
        UserGroupInformation.getCurrentUser.getUserName
    }
    user.shouldBe("dummy")
  }

  it("can override login GUI on executors") {
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
