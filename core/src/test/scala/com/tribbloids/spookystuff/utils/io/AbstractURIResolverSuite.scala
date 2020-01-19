package com.tribbloids.spookystuff.utils.io

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.testutils.{FunSpecx, LocalPathDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.CommonConst
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object AbstractURIResolverSuite {

  case class Semaphore(
      @transient sc: SparkContext,
      @transient _ii: AtomicInteger = new AtomicInteger(0)
  ) {

    val ii: Broadcast[AtomicInteger] = sc.broadcast(_ii)

    def get(): Seq[String] = {
      val v = ii.value.incrementAndGet()
      if (v == 1) Nil
      else Seq(s"value should == 1, but it is $v")

    }

    def give(): Seq[String] = {
      val v = ii.value.getAndDecrement()
      if (v == 1) Nil
      else Seq(s"value should == 1, but it is $v")
    }
  }
}

/**
  * Created by peng on 07/10/15.
  */
abstract class AbstractURIResolverSuite extends FunSpecx with LocalPathDocsFixture {

  @transient val resolver: URIResolver
  @transient val schemaPrefix: String

  def temp(path: String): TempResource = TempResource(
    resolver,
    path
  )

  lazy val existingFile: TempResource = temp("a-file.txt")
  lazy val nonExistingFile: TempResource = temp("not-a-file.txt")

  lazy val nonExistingDir: TempResource = temp("non-existing")
  lazy val nonExistingSubFile: TempResource = temp("non-existing/not-a-file.txt")

  lazy val nonExistingAbsolute: TempResource = temp("/temp/non-existing/not-a-file.txt")

  val sc: SparkContext = TestHelper.TestSC

  it("can convert relative path of non-existing file") {
    val abs = resolver.toAbsolute(nonExistingSubFile.pathStr)
    assert(abs == schemaPrefix + CommonConst.USER_DIR + "/" + nonExistingSubFile.pathStr)
  }

  it("can convert absolute path of non-existing file") {
    val abs = resolver.toAbsolute(nonExistingAbsolute.pathStr)
    assert(abs == schemaPrefix + nonExistingAbsolute.pathStr)
  }

  it(".toAbsolute is idempotent") {
    val once = resolver.toAbsolute(nonExistingFile.pathStr)
    val twice = resolver.toAbsolute(once)
    assert(once === twice)
    assert(once.startsWith(schemaPrefix))
    if (once.contains("file:")) assert(once.split("file:").head.isEmpty)
  }

  it(".resourceOrAbsolute is idempotent") {
    val once = resolver.resourceOrAbsolute(nonExistingFile.pathStr)
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

  it("resolver is serializable") {

    AssertSerializable(
      resolver,
      condition = { (v1: URIResolver, v2: URIResolver) =>
        v1.toString == v2.toString
      }
    )
  }

  describe("input") {

    it("can get metadata concurrently") {

      val rdd = sc.parallelize(1 to 100, 10)

      val resolver: URIResolver = this.resolver
      val HTML_URL = this.HTML_URL
      val mdRDD = rdd.map { i =>
        val md = resolver.input(HTML_URL) { _.metadata.all }
        md
      }
      val mds = mdRDD.collect().map {
        _.asMap.filterNot(_._1.contains("Space")).map(identity)
      }

      AssertSerializable(mds.head)
      assert(mds.head == mds.last)
    }
  }

  describe("output") {

//    it("can get metadata concurrently") {
//
//      val rdd = sc.parallelize(1 to 100, 10)
//
//      val resolver: URIResolver = this.resolver
//      val HTML_URL = this.HTML_URL
//      val mdRDD = rdd.map { i =>
//        val md = resolver.output(HTML_URL, WriteMode.CreateOnly) { _.metadata.all }
//        md
//      }
//      val mds = mdRDD.collect().map {
//        _.asMap.filterNot(_._1.contains("Space")).map(identity)
//      }
//
//      AssertSerializable(mds.head)
//      assert(mds.head == mds.last)
//    }

    it("can automatically create missing directory") {

      nonExistingDir.requireVoid {

        resolver.output(nonExistingSubFile.pathStr, WriteMode.CreateOnly) { out =>
          out.stream
        }

        resolver.input(nonExistingDir.pathStr) { in =>
          assert(in.isExisting)
        }
      }
    }

    it("can not overwrite over existing file") {

      existingFile.requireEmptyFile {

        for (i <- 1 to 3) {
          intercept[IOException] {
            resolver.output(existingFile.pathStr, WriteMode.CreateOnly) { out =>
              out.stream
            }
          }
        }
      }
    }

    it("Cannot grant multiple OutputStreams for 1 file") {

      existingFile.requireVoid {

        resolver.output(existingFile.pathStr, WriteMode.CreateOnly) { out1 =>
          out1.stream
          resolver.output(existingFile.pathStr, WriteMode.CreateOnly) { out2 =>
            intercept[IOException] {

              out2.stream
//              println(out1.stream)
//              println(out2.stream)
            }
          }
        }
      }
    }

    describe("copyTo") {
      it("a new file") {
        existingFile.requireRandomFile(16) {
          nonExistingFile.requireVoid {

            resolver.newSession(existingFile.pathStr).copyTo(nonExistingFile.pathStr, WriteMode.CreateOnly)

            val copied = resolver.input(nonExistingFile.pathStr)(_.getLength == 16)
            assert(copied)
          }
        }
      }

      it("overwrite an existing file") {
        existingFile.requireRandomFile(16) {
          nonExistingFile.requireRandomFile(16) {

            resolver.newSession(existingFile.pathStr).copyTo(nonExistingFile.pathStr, WriteMode.Overwrite)

            val copied = resolver.input(nonExistingFile.pathStr)(_.getLength == 16)
            assert(copied)
          }
        }
      }
    }

  }

  describe("Snapshot") {

    def testSnapshotIO(
        url: String,
        toBeWritten: Seq[Byte]
    ): Unit = {

      try {
        val rdd = sc.parallelize(1 to 100, 100)

        val resolver: URIResolver = this.resolver

        rdd.foreach { i =>
          val snapshot = Snapshot(resolver.newSession(url))
          snapshot.during { session =>
            val bytes: Array[Byte] = session.input { in =>
              if (in.isExisting) IOUtils.toByteArray(in.stream)
              else Array.empty
            }

            val lastByte: Byte = bytes.toSeq.lastOption.getOrElse(0)

            val withExtra = bytes :+ (lastByte + 1).byteValue()

//            println(s"write ${bytes.length} => ${withExtra.length}")

            session.output(WriteMode.Overwrite) { out =>
              val stream = out.stream
              stream.write(withExtra)
            }
          }
        }

        Predef.assert(!resolver.isAlreadyExisting(HTML_URL + ".lock")())

        val bytes = resolver.input(url) { in =>
          IOUtils.toByteArray(in.stream)
        }

        assert(bytes.length === toBeWritten.size)
        assert(bytes.toSeq == toBeWritten)

      } finally {

        resolver.newSession(url).delete(false)
      }
    }

    it("can guarantee sequential read and write on existing file") {
      existingFile.requireEmptyFile {

        existingFile.session.output(WriteMode.Overwrite) { out =>
          out.stream.write(Array(10.byteValue()))
        }

        val groundTruth = (10 to 110).map(_.byteValue())
        testSnapshotIO(existingFile.session.absolutePathStr, groundTruth)
      }
    }

    it("can guarantee sequential read and write on non-existing file") {
      existingFile.requireVoid {

        val groundTruth = (1 to 100).map(_.byteValue())
        testSnapshotIO(existingFile.session.absolutePathStr, groundTruth)
      }
    }
  }

//  ignore("Lock") {
//
//    def testLock(url: String) = {
//      val rdd = sc.parallelize(1 to 100, 100)
//
//      val resolver: URIResolver = this.resolver
////      val logger = LoggerFactory.getLogger(this.getClass)
//
//      val ss = Semaphore(sc)
//      val errors = rdd
//        .map { i =>
////        logger.info(s"locking: $i")
//
//          resolver.lockAccessDuring(url) { _ =>
//            val r1 = ss.get()
//            Thread.sleep(Random.nextInt(10))
//            val r2 = ss.give()
//            r1 ++ r2
//          }
//        }
//        .collect()
//        .flatten
//        .toSeq
//
//      assert(errors.isEmpty)
//
//      assert(!resolver.isAlreadyExisting(HTML_URL + ".lock")())
//    }
//
//    def testLockIO(url: String): Unit = {
//
//      try {
//        val rdd = sc.parallelize(1 to 1000, 100)
//
//        val resolver: URIResolver = this.resolver
////        val logger = LoggerFactory.getLogger(this.getClass)
//
//        rdd.foreach { i =>
////          logger.info(s"locking: $i")
//
//          resolver.lockAccessDuring(url) { lockedPath =>
//            val bytes: Array[Byte] = resolver.input(lockedPath, unlockFirst = false) { in =>
//              if (in.isExisting) IOUtils.toByteArray(in.stream)
//              else Array.empty
//            }
//
//            val lastByte: Byte = bytes.toSeq.lastOption.getOrElse(0: Byte)
//
//            resolver.output(lockedPath, WriteMode.Overwrite) { out =>
//              val stream = out.stream
//              stream.write(bytes)
//              stream.write(lastByte + 1)
//              stream.flush()
//              stream.close()
//            }
//          }
//        }
//
//        Predef.assert(!resolver.isAlreadyExisting(HTML_URL + ".lock")())
//
//        val bytes = resolver.input(url) { in =>
//          IOUtils.toByteArray(in.stream)
//        }
//
//        val length = rdd.count()
//        assert(bytes.length === length)
//
//        assert(
//          bytes.toSeq ==
//            (1 to length.intValue()).map(_.byteValue())
//        )
//
//      } finally {
//
//        resolver.newSession(url).delete(false)
//      }
//    }
//
////    it("can guarantee sequential locking") {
////      testLock(HTML_URL)
////    }
//
//    it("... even for non existing path") {
//      resolver.newSession(nonExisting).delete(false)
//
//      testLock(nonExisting)
//    }
//
//    it("can guarantee sequential read and write") {
//      resolver.newSession(existing).delete(false)
//
//      testLockIO(existing)
//    }
//  }
}
