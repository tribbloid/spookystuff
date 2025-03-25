package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import ai.acyclic.prover.commons.util.{Causes, Retry}
import com.tribbloids.spookystuff.commons.{CommonUtils, TreeException}
import com.tribbloids.spookystuff.io.{LocalResolver, URLConnectionResolver, WriteMode}
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.{SparkContext, SparkFiles}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.attribute.PosixFilePermission
import java.util.UUID
import scala.util.Try

trait BinaryDeployment extends Serializable {
  import BinaryDeployment.*

  /**
    * can deploy locally or for the cluster through SparkContext.addFile
    *
    * can be serialised & shipped to workers, but SparkCOntext will be lost
    */

  def repositoryURI: String
  def targetPath: String

  def getLocalFilePath(trySparkFile: Boolean) = {

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

  /**
    *   - return the path immediately if the local file already exists
    *   - if not eixsting, use SparkFiles.get to get a file replica and copy it to the path
    *   - if not possible, download it to the path
    */
  @transient lazy val localFilePath: String =
    getLocalFilePath(true)

  object Local extends Serializable {

    def verify: String = verifyFile(targetPath).get

    def download(): String = {
      // download from remoteURI to localURI
      val remoteResolver = URLConnectionResolver(10000)
      val localResolver = LocalExeResolver

      remoteResolver.input(repositoryURI) { i =>
        localResolver.output(targetPath, WriteMode.Overwrite) { o =>
          IOUtils.copy(i.stream, o.stream)
        }
      }

      verify
    }

    def copyFromSparkFile(): Option[Any] = {

      val srcStr = SparkFiles.get(sparkFileName)
      val srcFile = new File(srcStr)
      val dstFile = new File(targetPath)
      SpookyUtils.ifFileNotExist(targetPath) {
        SpookyUtils.treeCopy(srcFile.toPath, dstFile.toPath)
      }
    }

  }

  final val sparkFileName: String = CommonUtils.inferFileNameFromURI(targetPath) + UUID.randomUUID().toString

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
          getLocalFilePath(false)
        }

        LoggerFactory.getLogger(this.getClass).info(s"Deploying `$sparkFileName` from `$fileDownloaded`")
        sparkContext.addFile(fileDownloaded)

        require(binaryExists, "Deploying `$localFileName` from `$fileDownloaded` is not effective")
      }
    }
  }
}

object BinaryDeployment {

  object LocalExeResolver extends LocalResolver(extraPermissions = Set(PosixFilePermission.OWNER_EXECUTE))

  val MIN_SIZE_K: Double = 1024.0 * 60

  def verifyFile(pathStr: String): Try[String] = Try {
    val isExists = LocalResolver.default.on(pathStr).satisfy { v =>
      v.getLength >= MIN_SIZE_K * 1024
    }
    assert(isExists, s"PhantomJS executable at $pathStr doesn't exist")
    pathStr
  }
}
