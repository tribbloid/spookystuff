package com.tribbloids.spookystuff.utils.io

import java.beans.Transient

import com.tribbloids.spookystuff.testutils.{FunSpecx, LocalPathDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.CommonConst
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

import scala.util.Random

object LocalResolverSuite {

  val nonExisting = s"test.txt"
  val nonExistingRelative = "non-existing/not-a-file.txt"
  val nonExistingAbsolute = "/non-existing/not-a-file.txt"
}

/**
  * Created by peng on 07/10/15.
  */
class LocalResolverSuite extends FunSpecx with LocalPathDocsFixture {

  import LocalResolverSuite._

  @transient lazy val resolver: URIResolver = LocalResolver
  @transient lazy val schemaPrefix = ""

  val sc = TestHelper.TestSC

  it("can convert relative path of non-existing file") {
    val abs = resolver.toAbsolute(nonExistingRelative)
    assert(abs == schemaPrefix + CommonConst.USER_DIR + "/" + nonExistingRelative)
  }

  it("can convert absolute path of non-existing file") {
    val abs = resolver.toAbsolute(nonExistingAbsolute)
    assert(abs == schemaPrefix + nonExistingAbsolute)
  }

  it(".toAbsolute is idempotent") {
    val once = resolver.toAbsolute(nonExisting)
    val twice = resolver.toAbsolute(once)
    assert(once === twice)
    assert(once.startsWith(schemaPrefix))
    if (once.contains("file:")) assert(once.split("file:").head.isEmpty)
  }

  it(".resourceOrAbsolute is idempotent") {
    val once = resolver.resourceOrAbsolute(nonExisting)
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

  it(".lockAccessDuring() can guarantee sequential access") {
    testLockAccess(HTML_URL)
  }
  it("... even for non existing path") {
    testLockAccess(nonExisting)
  }

  private def testLockAccess(url: String) = {
    val rdd = sc.parallelize(1 to 100, 4)

    @Transient var ss = 0

    val resolver: URIResolver = this.resolver
    rdd.foreach { i =>
      resolver.lockAccessDuring(url) { lockedPath =>
        ss += 1
        Predef.assert(ss == 1)
        Thread.sleep(Random.nextInt(1000))
        ss -= 1
      }
    }

    assert(!resolver.isAlreadyExisting(HTML_URL + ".lock")())
  }

  it(".input can get metadata concurrently") {

    val rdd = sc.parallelize(1 to 100, 10)

    val resolver: URIResolver = this.resolver
    val HTML_URL = this.HTML_URL
    val mdRDD = rdd.map { i =>
      val md = resolver.input(HTML_URL) { _.allMetadata }
      md
    }
    val mds = mdRDD.collect().map {
      _.self.filterNot(_._1.contains("Space"))
    }

    AssertSerializable(mds.head)
    assert(mds.head == mds.last)
  }

  it(".output can get metadata concurrently") {

    val rdd = sc.parallelize(1 to 100, 10)

    val resolver: URIResolver = this.resolver
    val HTML_URL = this.HTML_URL
    val mdRDD = rdd.map { i =>
      val md = resolver.output(HTML_URL, overwrite = false) { _.allMetadata }
      md
    }
    val mds = mdRDD.collect().map {
      _.self.filterNot(_._1.contains("Space"))
    }

    AssertSerializable(mds.head)
    assert(mds.head == mds.last)
  }
}
