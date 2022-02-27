package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.utils.CommonConst
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.BeforeAndAfterAll

object HDFSResolverSuite_MiniCluster {

  lazy val conf = {
    val conf = new Configuration()
    conf.set("fs.default.name", "hdfs:///")
    conf
  }
}

class HDFSResolverSuite_MiniCluster extends AbstractHDFSResolverSuite with BeforeAndAfterAll {

  override lazy val confFactory = () => HDFSResolverSuite_MiniCluster.conf

  lazy val miniCluster = {

    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, CommonConst.USER_TEMP_DIR)

    val builder = new MiniDFSCluster.Builder(HDFSResolverSuite_MiniCluster.conf)
    val result = builder.build()
    result
  }

  @transient override lazy val schemaPrefix = {
    miniCluster.getURI.toString
  }

  override def userDir: String = s"/user/${UserGroupInformation.getCurrentUser.getUserName}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    miniCluster
  }

  override def afterAll(): Unit = {
    miniCluster.shutdown()
    super.afterAll()
  }

}
