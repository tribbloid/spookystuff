package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import ai.acyclic.prover.commons.util.{Causes, Retry}
import com.tribbloids.spookystuff.commons.{CommonUtils, TreeException}
import com.tribbloids.spookystuff.io.{URLConnectionResolver, WriteMode}
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.{SparkContext, SparkFiles}
import org.slf4j.LoggerFactory

import java.nio.file.Path
import com.tribbloids.spookystuff.io.{FilePermissionType, LocalResolver}
import java.util.UUID
import scala.util.Try

trait BinaryDeployment extends Serializable {
  import BinaryDeployment.*

  final val sparkFileName: String = CommonUtils.inferFileNameFromPath(targetPath) + UUID.randomUUID().toString

  /**
    *   - return the path immediately if the local file already exists
    *   - if not eixsting, use SparkFiles.get to get a file replica and copy it to the path
    *   - if not possible, download it to the path
    */
  @transient lazy val deployOnce: String =
    deploy(true)

  /**
    * can deploy locally or for the cluster through SparkContext.addFile
    *
    * can be serialised & shipped to workers, but SparkCOntext will be lost
    */

  def targetPath: Path

  def deploy(trySparkFile: Boolean = true): String = {

    def existing = { () =>
      Local.verify
    }

    def fromSparkFile = { () =>
      Local.copyFromSparkFile()
      Local.verify
    }

    def fromRepository = { () =>
      Local.download()
      Local.verify
    }

    val allAttempts =
      if (trySparkFile) Seq(existing, fromSparkFile, fromRepository)
      else Seq(existing, fromRepository)

    val result: Option[String] = TreeException.|||^(
      allAttempts,
      agg = { seq =>
        new UnsupportedOperationException(
          s"${this.getClass.getSimpleName} cannot find resource for deployment, " +
            s"please provide Internet Connection or deploy manually",
          Causes.combine(seq)
        )
      }
    )
    result.get
  }

  protected def downloadWithoutVerify(): Unit

  case class OnSparkDriver(sparkContext: SparkContext) extends NOTSerializable {

    // can only be called on worker
    lazy val addSparkFile: Unit = {

      val binaryExists: Boolean = {

        val existingFiles = sparkContext.listFiles()
        val result = existingFiles.exists(ss => ss.endsWith(sparkFileName))
        result
      }

      if (binaryExists) {

        LoggerFactory.getLogger(this.getClass).info(s"Source `$sparkFileName` is already already deployed")
      } else {

        val fileDownloaded = Retry.FixedInterval(3) {
          deploy(false)
        }

        LoggerFactory.getLogger(this.getClass).info(s"Deploying `$sparkFileName` from `$fileDownloaded`")
        sparkContext.addFile(fileDownloaded)

        require(binaryExists, "Deploying `$localFileName` from `$fileDownloaded` is not effective")
      }
    }
  }

  object Local extends Serializable {

    def copyFromSparkFile(): Option[Any] = {

      val srcStr = SparkFiles.get(sparkFileName)
      val src = Path.of(srcStr)
      val dst = targetPath
      SpookyUtils.ifFileNotExist(targetPath) {
        SpookyUtils.treeCopy(src, dst)
      }
    }

    def download(): String = {
      downloadWithoutVerify()

      verify
    }

    def verify: String = verifyFile(targetPath).get

  }
}

object BinaryDeployment {

  val MIN_SIZE_M: Double = 4
  val MIN_SIZE_B: Double = MIN_SIZE_M * 1024 * 1024

  def verifyFile(path: Path): Try[String] = Try {
    val pathString = path.toString
    val isExists = LocalResolver.default.on(pathString).satisfy { v =>
      val length = v.getLength
      length >= MIN_SIZE_B
    }
    assert(isExists, s"Binary executable at $pathString doesn't exist")
    pathString
  }

  trait StaticRepository extends BinaryDeployment {

    def repositoryURI: String

    protected def downloadWithoutVerify(): Unit = {
      // download from remoteURI to localURI
      val remoteResolver = URLConnectionResolver(10000)
      val localResolver = LocalExeResolver

      remoteResolver.input(repositoryURI) { i =>
        localResolver.output(targetPath.toString, WriteMode.Overwrite) { o =>
          IOUtils.copy(i.stream, o.stream)
        }
      }

    }

  }

  object LocalExeResolver extends LocalResolver(writePermission = Set(FilePermissionType.Executable))
}
