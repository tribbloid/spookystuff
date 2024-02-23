package com.tribbloids.spookystuff.io

import ai.acyclic.prover.commons.spark.Envs
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SparkEnvSpec}
import com.tribbloids.spookystuff.commons.serialization.AssertSerializable
import com.tribbloids.spookystuff.io.AbstractURIResolverSuite.SequentialCheck
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import scala.util.{Random, Try}

object AbstractURIResolverSuite {

  case class SequentialCheck(
      @transient sc: SparkContext,
      @transient _ii: AtomicInteger = new AtomicInteger(0),
      max: Int = 1
  ) {

    val iiB: Broadcast[AtomicInteger] = sc.broadcast(_ii)
    def ii: AtomicInteger = iiB.value

    def acquire(): Try[Unit] = synchronized {
      Try {
        val v = ii.incrementAndGet()
        if (v > max)
          throw new UnsupportedOperationException(s"cannot acquire - semaphore value $v is out of range")
      }
    }

    def release(): Try[Unit] = synchronized {
      Try {
        val v = ii.getAndDecrement()
        if (v > max || v <= 0)
          throw new UnsupportedOperationException(s"released - semaphore value $v is out of range")
      }
    }
  }
}

/**
  * Created by peng on 07/10/15.
  */
abstract class AbstractURIResolverSuite extends SparkEnvSpec with FileDocsFixture {

  @transient val resolver: URIResolver
  @transient val schemaPrefix: String

  val numWrites: Int = 1000

  lazy val temp: TempResource = TempResource(
    resolver,
    Envs.TEMP
  )

  lazy val existingFile: TempResource = temp \ "a-file.txt"
  lazy val nonExistingFile: TempResource = temp \ "not-a-file.txt"

  lazy val dir: TempResource = temp \ "dir"
  lazy val nonExistingSubFile: TempResource = dir \ "not-a-file.txt"

  object Absolute {

    lazy val nonExistingDir: TempResource = TempResource(
      resolver,
      "/temp/non-existing"
    )
    lazy val nonExistingSubFile: TempResource = nonExistingDir \ "not-a-file.txt"

  }

  it("can convert relative path of non-existing file") {
    {
      val abs = resolver.toAbsolute(nonExistingFile.pathStr)
      assert(abs == schemaPrefix + Envs.USER_DIR + "/" + nonExistingFile.pathStr)
    }

    {
      val abs = resolver.toAbsolute(nonExistingSubFile.pathStr)
      assert(abs == schemaPrefix + Envs.USER_DIR + "/" + nonExistingSubFile.pathStr)
    }
  }

  it("can convert absolute path of non-existing file") {
    val abs = resolver.toAbsolute(Absolute.nonExistingSubFile.pathStr)
    assert(abs == schemaPrefix + Absolute.nonExistingSubFile.pathStr)
  }

  it(".toAbsolute is idempotent") {
    val once = resolver.toAbsolute(nonExistingFile.pathStr)
    val twice = resolver.toAbsolute(once)
    assert(once === twice)
    assert(once.startsWith(schemaPrefix))
    if (once.contains("file:")) assert(once.split("file:").head.isEmpty)
  }

//  it(".resourceOrAbsolute is idempotent") {
//    val once = resolver.resourceOrAbsolute(nonExistingFile.pathStr)
//    val twice = resolver.resourceOrAbsolute(once)
//    assert(once === twice)
//    assert(once.startsWith(schemaPrefix))
//    if (once.contains("file:")) assert(once.split("file:").head.isEmpty)
//  }

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
      val mdRDD = rdd.map { _ =>
        val md = resolver.input(HTML_URL)(_.metadata.all)
        md
      }
      val mds = mdRDD
        .collect()
        .map { v =>
          v.KVs.raw.filterNot(_._1.contains("Space"))
        }
        .toList

      AssertSerializable(mds.head)
      assert(mds.distinct.size == 1)
    }

    it("all accessors can be mutated after creation") {

      def accessorVs(rr: Resource#InputView) = {
        Seq(
          rr.getLength,
          rr.getLastModified
        )
      }

      existingFile.requireEmptyFile {

        val session = resolver.execute(existingFile.pathStr)

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

    it("can automatically create missing directory") {

      dir.requireVoid {

        resolver.output(nonExistingSubFile.pathStr, WriteMode.CreateOnly) { out =>
          out.stream
        }

        resolver.input(dir.pathStr) { in =>
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

    it("cannot grant multiple OutputStreams for 1 file") {

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
        existingFile.requireRandomContent(16) {
          nonExistingFile.requireVoid {

            resolver.execute(existingFile.pathStr).copyTo(nonExistingFile.pathStr, WriteMode.CreateOnly)

            val copied = resolver.input(nonExistingFile.pathStr)(_.getLength == 16)
            assert(copied)
          }
        }
      }

      it("overwrite an existing file") {
        existingFile.requireRandomContent(16) {
          nonExistingFile.requireRandomContent(16) {

            resolver.execute(existingFile.pathStr).copyTo(nonExistingFile.pathStr, WriteMode.Overwrite)

            val copied = resolver.input(nonExistingFile.pathStr)(_.getLength == 16)
            assert(copied)
          }
        }
      }
    }

  }

  it("move 1 file to the same target should be sequential") {

    existingFile.requireEmptyFile {
      val pathStr = existingFile.absolutePathStr
      val rdd = sc.parallelize(1 to numWrites, numWrites)

      val resolver: URIResolver = this.resolver

      val ss = SequentialCheck(sc)

      val errors = rdd
        .map { _ =>
          resolver.retry {

            resolver
              .execute(pathStr)
              .moveTo(pathStr + ".moved")

            val r1 = ss.acquire().failed.toOption.toSeq
            Thread.sleep(Random.nextInt(10))
            val r2 = ss.release().failed.toOption.toSeq

            resolver
              .execute(pathStr + ".moved")
              .moveTo(pathStr)

            r1 ++ r2
          }
        }
        .collect()
        .flatten
        .toSeq

      assert(errors.isEmpty, errors.mkString("\n"))
    }
  }

  it("move 1 file to different targets should be sequential") {

    existingFile.requireEmptyFile {
      val pathStr = existingFile.absolutePathStr
      val rdd = sc.parallelize(1 to numWrites, numWrites)

      val resolver: URIResolver = this.resolver

      val ss = SequentialCheck(sc)

      val errors = rdd
        .map { i =>
          resolver.retry {

            resolver
              .execute(pathStr)
              .moveTo(pathStr + i + ".moved")

            val r1 = ss.acquire().failed.toOption.toSeq
            Thread.sleep(Random.nextInt(10))
            val r2 = ss.release().failed.toOption.toSeq

            resolver
              .execute(pathStr + i + ".moved")
              .moveTo(pathStr)

            r1 ++ r2
          }
        }
        .collect()
        .flatten
        .toSeq

      assert(errors.isEmpty, errors.mkString("\n"))
    }
  }

  // TODO: doesn't work, no guarantee
  ignore("move different files to the same target should be sequential") {
    existingFile.requireEmptyFile {
      val pathStr = existingFile.absolutePathStr
      val rdd = sc.parallelize(1 to numWrites, numWrites)

      val resolver: URIResolver = this.resolver

      val ss = SequentialCheck(sc)
      val errors = rdd
        .map { i =>
          resolver.retry {

            val src = resolver
              .execute(pathStr + s"${Random.nextLong()}")

            try {

//              src.createNew()
              src.output(WriteMode.CreateOnly) { oo =>
                oo.stream.write(("" + i).getBytes)
              }
              src.moveTo(pathStr)
            } catch {
              case e: Exception =>
                src.delete(false)
                throw e
            }

            val r1 = ss.acquire().failed.toOption.toSeq
            Thread.sleep(Random.nextInt(10))
            val r2 = ss.release().failed.toOption.toSeq

            resolver
              .execute(pathStr + ".moved")
              .delete(false)

            r1 ++ r2
          }
        }
        .collect()
        .flatten
        .toSeq

      assert(errors.isEmpty, errors.mkString("\n"))
    }
  }

  ignore("touch should be sequential") {

    existingFile.requireVoid {
      val pathStr = existingFile.pathStr
      val rdd = sc.parallelize(1 to numWrites, numWrites)

      val resolver: URIResolver = this.resolver

      val ss = SequentialCheck(sc)
      val errors = rdd
        .map { _ =>
          resolver.retry {
            resolver
              .execute(pathStr)
              .createNew()

            val r1 = ss.acquire().failed.toOption.toSeq
            Thread.sleep(Random.nextInt(10))
            val r2 = ss.release().failed.toOption.toSeq

            resolver
              .execute(pathStr)
              .delete()

            r1 ++ r2
          }
        }
        .collect()
        .flatten
        .toSeq

      assert(errors.isEmpty, errors.mkString("\n"))
    }
  }

  describe("Lock") {

    def doTest(
        url: String
    ): Unit = {

      val rdd = sc.parallelize(1 to numWrites, numWrites)

      val resolver: URIResolver = this.resolver

      val ss = SequentialCheck(sc)
      val errors = rdd
        .map { _ =>
          resolver.lock(url) { _ =>
            val r1 = ss.acquire().failed.toOption.toSeq
            Thread.sleep(Random.nextInt(10))
            val r2 = ss.release().failed.toOption.toSeq
            r1 ++ r2
          }
        }
        .collect()
        .flatten
        .toSeq

      assert(errors.isEmpty, errors.mkString("\n"))
    }

    describe("can guarantee sequential access") {
      it("to existing file") {
        existingFile.requireRandomContent() {

          doTest(existingFile.absolutePathStr)
        }
      }

      it("to empty directory") {

        dir.requireEmptyDir {
          doTest(dir.absolutePathStr)
        }
      }

      it("to non empty directory") {

        nonExistingSubFile.requireRandomContent() {
          doTest(dir.absolutePathStr)
        }
      }

      ignore("... even for non existing path") {
        nonExistingFile.requireVoid {

          doTest(nonExistingFile.absolutePathStr)
        }
      }
    }

    def doTestIO(
        url: String,
        groundTruth: Seq[Byte]
    ): Unit = {

      try {
        val rdd = sc.parallelize(1 to numWrites, numWrites)

        val resolver: URIResolver = this.resolver

        rdd.foreach { _ =>
          resolver.lock(url) { exe =>
            val bytes: Array[Byte] = exe.input { in =>
              if (in.isExisting) IOUtils.toByteArray(in.stream)
              else Array.empty
            }

            val lastByte: Byte = bytes.toSeq.lastOption.getOrElse(0)

            val withExtra = bytes :+ (lastByte + 1).byteValue()

            //            println(s"write ${bytes.length} => ${withExtra.length}")

            exe.output(WriteMode.Overwrite) { out =>
              val stream = out.stream
              stream.write(withExtra)
            }
          }
        }

        Thread.sleep(2000)

        val bytes = resolver
          .input(url) { in =>
            IOUtils.toByteArray(in.stream)
          }
          .toSeq

        val truncated = bytes.slice(0, groundTruth.size)

        assert(
          s"${truncated.size} elements:\n ${truncated.mkString(" ")}" ===
            s"${groundTruth.size} elements:\n ${groundTruth.mkString(" ")}"
        )
        assert(truncated.length === groundTruth.size)

      } finally {

        resolver.execute(url).delete(false)
      }
    }

    describe("can guarantee sequential read and write") {

      it("to existing file") {
        existingFile.requireEmptyFile {

          existingFile.execution.output(WriteMode.Overwrite) { out =>
            out.stream.write(Array(10.byteValue()))
          }

          val groundTruth = (10 to numWrites + 10).map(_.byteValue())
          doTestIO(existingFile.execution.absolutePathStr, groundTruth)
        }
      }

      ignore("to non-existing file") {
        nonExistingFile.requireVoid {

          val groundTruth = (1 to numWrites).map(_.byteValue())
          doTestIO(nonExistingFile.execution.absolutePathStr, groundTruth)
        }
      }
    }
  }

}
