package com.tribbloids.spookystuff.utils.io

import java.beans.Transient

import com.tribbloids.spookystuff.testutils.{FunSpecx, TestHelper}
import com.tribbloids.spookystuff.utils.CommonConst
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

import scala.util.Random

/**
  * Created by peng on 07/10/15.
  */
class LocalResolverSuite extends FunSpecx {

  @transient lazy val resolver: URIResolver = LocalResolver
  @transient lazy val schemaPrefix = ""

  val nonExistingRelativePath = "non-existing/not-a-file.txt"
  val nonExistingAbsolutePath = "/non-existing/not-a-file.txt"


  val sc = TestHelper.TestSC

  it("can convert relative path of non-existing file") {
    val abs = resolver.toAbsolute(nonExistingRelativePath)
    assert(abs == schemaPrefix + CommonConst.USER_DIR +"/"+ nonExistingRelativePath)
  }

  it("can convert absolute path of non-existing file") {
    val abs = resolver.toAbsolute(nonExistingAbsolutePath)
    assert(abs == schemaPrefix + nonExistingAbsolutePath)
  }

  it("resolver.toAbsolute is idempotent") {
    val once = resolver.toAbsolute("TestPolicies.xml")
    val twice = resolver.toAbsolute(once)
    assert(once === twice)
    assert(once.startsWith(schemaPrefix))
    if (once.contains("file:")) assert(once.split("file:").head.isEmpty)
  }

  it("resolver.resourceOrAbsolute is idempotent") {
    val once = resolver.resourceOrAbsolute("TestPolicies.xml")
    val twice = resolver.resourceOrAbsolute(once)
    assert(once === twice)
    assert(once.startsWith(schemaPrefix))
    if (once.contains("file:")) assert(once.split("file:").head.isEmpty)
  }

  //  it("ResourceFirstResolver can convert path to non-existing file to absolute") {
  //    val resolver = ResourceFirstResolver(HDFSResolver(new Configuration()))
  //    val abs = resolver.toAbsolute(nonExistingPath)
  //    assert(abs == "file:"+CommonConst.USER_DIR +"/"+ nonExistingPath)
  //  }

  it("resolver is Kryo serializable") {
    //    val resolver: HDFSResolver = HDFSResolver(new Configuration())

    val serDe = new KryoSerializer(new SparkConf()).newInstance()

    val serialized = serDe.serialize(resolver)

    val des = serDe.deserialize[resolver.type](serialized)

    assert(resolver.toString == des.toString)
  }

  it("resolver.lockAccessDuring() can guarantee sequential access") {
    val rdd = sc.parallelize(1 to 100, 4)

    @Transient var ss = 0

    val path = "TestPolicies.xml"
    val resolver: URIResolver = this.resolver
    rdd.foreach {
      i =>
        resolver.lockAccessDuring(path) {
          lockedPath =>
            ss += 1
            Predef.assert(ss == 1)
            Thread.sleep(Random.nextInt(1000))
            ss -=1
        }
    }
  }
}
