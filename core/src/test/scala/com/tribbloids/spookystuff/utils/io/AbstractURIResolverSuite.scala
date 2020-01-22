package com.tribbloids.spookystuff.utils.io

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger

import com.tribbloids.spookystuff.testutils.{FunSpecx, LocalPathDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.{CommonConst, Retry}
import com.tribbloids.spookystuff.utils.io.AbstractURIResolverSuite.Semaphore
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.util.Random

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

  val numConcurrentWrites = 1000

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

  describe("Session") {}

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

    it("all accessors can be mutated after creation") {

      def accessorVs(rr: InputResource) = {
        Seq(
          rr.getLength,
          rr.getLastModified
        )
      }

      existingFile.requireEmptyFile {

        val session = resolver.newSession(existingFile.pathStr)

        var vs1 = session.input(accessorVs)

        for (i <- 1 to 3) {
          resolver.output(existingFile.pathStr, WriteMode.Overwrite) { out =>
            val a = Array.ofDim[Byte](16 * i)
            Random.nextBytes(a)

            out.stream.write(a)
          }

          val vs2 = session.input(accessorVs)

          assert(vs1 != vs2)

          vs1 = vs2
        }
      }
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

    def testConcurrentIO(
        url: String,
        groundTruth: Seq[Byte]
    ): Unit = {

      try {
        val rdd = sc.parallelize(1 to numConcurrentWrites, numConcurrentWrites)

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

        Predef.assert(!resolver.isAlreadyExisting(url + ".lock")())

        val bytes = resolver
          .input(url) { in =>
            IOUtils.toByteArray(in.stream)
          }
          .toSeq

        assert(
          s"${bytes.size} elements:\n ${bytes.mkString(" ")}" ===
            s"${groundTruth.size} elements:\n ${groundTruth.mkString(" ")}"
        )
        assert(bytes.length === groundTruth.size)

      } finally {

        resolver.newSession(url).delete(false)
      }
    }

    it("can guarantee sequential read and write on existing file") {
      existingFile.requireEmptyFile {

        existingFile.session.output(WriteMode.Overwrite) { out =>
          out.stream.write(Array(10.byteValue()))
        }

        val groundTruth = (10 to numConcurrentWrites + 10).map(_.byteValue())
        testConcurrentIO(existingFile.session.absolutePathStr, groundTruth)
      }
    }

    it("can guarantee sequential read and write on non-existing file") {
      nonExistingFile.requireVoid {

        val groundTruth = (1 to numConcurrentWrites).map(_.byteValue())
        testConcurrentIO(nonExistingFile.session.absolutePathStr, groundTruth)
      }
    }
  }

  it("move should be atomic") {

    existingFile.requireEmptyFile {
      val pathStr = existingFile.pathStr
      val rdd = sc.parallelize(1 to numConcurrentWrites, numConcurrentWrites)

      val resolver: URIResolver = this.resolver

      val ss = Semaphore(sc)
      val errors = rdd
        .map { i =>
          Retry.FixedInterval(n = 30, interval = 1000, silent = true) {
            resolver
              .newSession(pathStr)
              .moveTo(pathStr + ".moved")

            val r1 = ss.get()
            Thread.sleep(Random.nextInt(10))
            val r2 = ss.give()

            resolver
              .newSession(pathStr + ".moved")
              .moveTo(pathStr)

            r1 ++ r2
          }
        }
        .collect()
        .flatten
        .toSeq

      assert(errors.isEmpty)
    }
  }

  ignore("move to a target should be atomic") {
    existingFile.requireEmptyFile {
      val pathStr = existingFile.pathStr
      val rdd = sc.parallelize(1 to numConcurrentWrites, numConcurrentWrites)

      val resolver: URIResolver = this.resolver

      val ss = Semaphore(sc)
      val errors = rdd
        .map { i =>
          Retry.FixedInterval(n = 30, interval = 1000, silent = true) {
            val src = resolver
              .newSession(pathStr + s"${Random.nextInt()}")

            src.touch()
            src.moveTo(pathStr)

            val r1 = ss.get()
            Thread.sleep(Random.nextInt(10))
            val r2 = ss.give()

            resolver
              .newSession(pathStr + ".moved")
              .delete(false)

            r1 ++ r2
          }
        }
        .collect()
        .flatten
        .toSeq

      assert(errors.isEmpty)
    }
  }

  ignore("touch should be atomic") {

    existingFile.requireVoid {
      val pathStr = existingFile.pathStr
      val rdd = sc.parallelize(1 to numConcurrentWrites, numConcurrentWrites)

      val resolver: URIResolver = this.resolver

      val ss = Semaphore(sc)
      val errors = rdd
        .map { i =>
          Retry.FixedInterval(n = 30, interval = 1000, silent = true) {
            resolver
              .newSession(pathStr)
              .touch()

            val r1 = ss.get()
            Thread.sleep(Random.nextInt(10))
            val r2 = ss.give()

            resolver
              .newSession(pathStr)
              .delete()

            r1 ++ r2
          }
        }
        .collect()
        .flatten
        .toSeq

      assert(errors.isEmpty)
    }
  }

  def testLock(): Unit = {

    // doesn't work for HDFS, so moved here
    describe("Lock") {

      def testConcurrent(
          url: String
      ): Unit = {

        val rdd = sc.parallelize(1 to numConcurrentWrites, numConcurrentWrites)

        val resolver: URIResolver = this.resolver

        val ss = Semaphore(sc)
        val errors = rdd
          .map { i =>
            resolver.lock(url).during { _ =>
              val r1 = ss.get()
              Thread.sleep(Random.nextInt(10))
              val r2 = ss.give()
              r1 ++ r2
            }
          }
          .collect()
          .flatten
          .toSeq

        assert(errors.isEmpty)
      }

      it("can guarantee sequential locking") {
        existingFile.requireEmptyFile {

          testConcurrent(existingFile.pathStr)
        }
      }

      it("... even for non existing path") {
        nonExistingFile.requireEmptyFile {

          testConcurrent(nonExistingFile.pathStr)
        }
      }

      def testConcurrentIO(
          url: String,
          groundTruth: Seq[Byte]
      ): Unit = {

        try {
          val rdd = sc.parallelize(1 to numConcurrentWrites, numConcurrentWrites)

          val resolver: URIResolver = this.resolver

          rdd.foreach { _ =>
            val lock = Lock(resolver.newSession(url))
            lock.during { session =>
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

          Predef.assert(!resolver.isAlreadyExisting(url + ".lock")())

          val bytes = resolver
            .input(url) { in =>
              IOUtils.toByteArray(in.stream)
            }
            .toSeq

          assert(
            s"${bytes.size} elements:\n ${bytes.mkString(" ")}" ===
              s"${groundTruth.size} elements:\n ${groundTruth.mkString(" ")}"
          )
          assert(bytes.length === groundTruth.size)

        } finally {

          resolver.newSession(url).delete(false)
        }
      }

      it("can guarantee sequential read and write on existing file") {
        existingFile.requireEmptyFile {

          existingFile.session.output(WriteMode.Overwrite) { out =>
            out.stream.write(Array(10.byteValue()))
          }

          val groundTruth = (10 to numConcurrentWrites + 10).map(_.byteValue())
          testConcurrentIO(existingFile.session.absolutePathStr, groundTruth)
        }
      }

      it("can guarantee sequential read and write on non-existing file") {
        nonExistingFile.requireVoid {

          val groundTruth = (1 to numConcurrentWrites).map(_.byteValue())
          testConcurrentIO(nonExistingFile.session.absolutePathStr, groundTruth)
        }
      }
    }
  }
}
