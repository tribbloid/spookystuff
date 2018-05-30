package com.tribbloids.spookystuff.utils.io

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

/**
 * Created by peng on 07/10/15.
 */
class LocalResolverSuite extends FunSpecx {

  val resolver: URIResolver = LocalResolver
  val schemaPrefix = ""

  val nonExistingRelativePath = "non-existing/not-a-file.txt"
  val nonExistingAbsolutePath = "/non-existing/not-a-file.txt"

  it("resolver can convert relative path of non-existing file") {
    val abs = resolver.toAbsolute(nonExistingRelativePath)
    assert(abs == schemaPrefix + System.getProperty("user.dir") +"/"+ nonExistingRelativePath)
  }

  it("resolver can convert absolute path of non-existing file") {
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
//    assert(abs == "file:"+System.getProperty("user.dir") +"/"+ nonExistingPath)
//  }

  it("resolver is Kryo serializable") {
//    val resolver: HDFSResolver = HDFSResolver(new Configuration())

    val serDe = new KryoSerializer(new SparkConf()).newInstance()

    val serialized = serDe.serialize(resolver)

    val des = serDe.deserialize[resolver.type](serialized)

    assert(resolver.toString == des.toString)
  }
}
