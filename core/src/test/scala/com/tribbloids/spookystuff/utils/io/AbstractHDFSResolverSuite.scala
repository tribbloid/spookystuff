package com.tribbloids.spookystuff.utils.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

object AbstractHDFSResolverSuite {

  lazy val conf = new Configuration()
}

/**
  * Created by peng on 07/10/15.
  */
abstract class AbstractHDFSResolverSuite extends AbstractURIResolverSuite {

  def confFactory: () => Configuration = () => AbstractHDFSResolverSuite.conf

  override val resolver: HDFSResolver = HDFSResolver(confFactory)

  val resolverWithUGI: HDFSResolver = HDFSResolver(
    confFactory,
    () => Some(UserGroupInformation.createUserForTesting("dummy", Array.empty))
  )

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

  it("can override login UGI") {

    val user: String = resolverWithUGI.input(existingFile.pathStr) { is =>
      UserGroupInformation.getCurrentUser.getUserName
    }
    user.shouldBe("dummy")
  }

  it("... on executors") {
    val resolver = this.resolverWithUGI

//    existingFile.requireEmptyFile {
//      resolver.output(existingFile.pathStr, WriteMode.Overwrite) { out =>
//        out.stream
//      }
//    }

    val pathStr = existingFile.pathStr

    val users = sc
      .parallelize(1 to (sc.defaultParallelism * 2))
      .mapPartitions { itr =>
        val str: String = resolver.input(pathStr) { is =>
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
