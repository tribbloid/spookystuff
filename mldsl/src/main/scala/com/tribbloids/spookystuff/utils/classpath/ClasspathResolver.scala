package com.tribbloids.spookystuff.utils.classpath

import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.utils.io.Resource.{DIR, FILE}
import com.tribbloids.spookystuff.utils.io.{ResourceMetadata, URIResolver, WriteMode, Resource => IOResource}
import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import io.github.classgraph.{ClassGraph, Resource, ScanResult}
import org.apache.commons.lang.StringUtils
import org.apache.spark.ml.dsl.utils.LazyVar

import java.io.{IOException, InputStream, OutputStream}
import java.nio.file.{Files, NoSuchFileException}
import java.util.regex.Pattern
import scala.collection.mutable
import scala.language.implicitConversions
import scala.tools.nsc.io.File

case class ClasspathResolver(
    elementsOverride: Option[Seq[String]] = None,
    classLoaderOverride: Option[ClassLoader] = None
) extends URIResolver {

  import scala.collection.JavaConverters._

  @transient lazy val metadataParser: ResourceMetadata.ReflectionParser[Resource] =
    ResourceMetadata.ReflectionParser[Resource]()

  lazy val graph: ClassGraph = {
    var base = new ClassGraph().enableClassInfo.ignoreClassVisibility

    elementsOverride.foreach { oo =>
      base = base.overrideClasspath(oo)
    }

    classLoaderOverride.foreach { oo =>
      base = base.overrideClassLoaders(oo)
    }

    base
  }

  lazy val dependencyRoots: Seq[String] = Seq(
    ".m2/repository",
    ".gradle/caches",
    "jre/lib",
    "jdk/lib"
  )

  def prunePath(v: String): String = {

    for (root <- dependencyRoots) {

      if (v.contains(root))
        return v.split(Pattern.quote(root)).last
    }

    v
  }

  trait UseScanResult extends Cleanable {

    // TODO: this may not be efficient as every new _Execution requires a new can, but for safety ...
    lazy val _scanResult: LazyVar[ScanResult] = LazyVar {
      graph.scan()
    }
    def scanned: ScanResult = _scanResult.value

    override def cleanImpl(): Unit = {
      _scanResult.peek.foreach { v =>
        v.close()
      }
    }
  }

  object _Execution extends (String => _Execution) {

    def apply(pathStr: String): _Execution = {

      lazy val scanned: ScanResult = graph.scan()
      val list = scanned.getResourcesWithPath(pathStr)

      val rOpt = list.asScala.headOption.map { v =>
        v
      }
      new _Execution(pathStr, rOpt)

    }
  }

  case class _Execution(
      pathStr: String,
      referenceOpt: Option[Resource]
  ) extends Execution
      with UseScanResult {

    lazy val childPattern: String = CommonUtils.\\\(pathStr, "*")
    lazy val offspringPattern: String = CommonUtils.\\\(pathStr, "**")

    def select(wildcard: String): Seq[_Execution] = {
      val list = scanned.getResourcesMatchingWildcard(wildcard)

      val result = list.asScala.map { v =>
        new _Execution(v.getPath, Some(v))
      }
      result
    }

    def referenceInfo: String = {
      val info = referenceOpt match {
        case Some(r) =>
          s"\tresource `$pathStr` refers to ${r.getURL.toString}"
        case None =>
          s"\tresource `$pathStr` has no reference"
      }
      info
    }

    override def absolutePathStr: String = pathStr

    case class _Resource(mode: WriteMode) extends IOResource {

      private lazy val ref = referenceOpt
        .getOrElse(
          throw new UnsupportedOperationException("reference doesn't exist")
        )

      override lazy val getURI: String = ref.getURI.toString

      override def getName: String = ref.getURI.getFragment

      override def getType: String =
        if (referenceOpt.nonEmpty) FILE
        else if (children.nonEmpty) DIR
        else throw new NoSuchFileException(s"File $pathStr doesn't exist")

      override lazy val children: Seq[_Execution] = select(childPattern)
      lazy val offspring: Seq[_Execution] = select(offspringPattern)

      override def getContentType: String = Files.probeContentType(ref.getClasspathElementFile.toPath)

      override def getLength: Long = ref.getLength

      override def getLastModified: Long = ref.getLastModified

      override protected def _metadata: ResourceMetadata = metadataParser(ref)

      override protected def _newIStream: InputStream = {
        try {
          ref.open()
        } catch {
          case e: Throwable =>
            throw new IOException(
              s"cannot read $getURI",
              e
            )
        }
      }

      override protected def _newOStream: OutputStream = unsupported("write")
    }

    override protected def _delete(mustExist: Boolean): Unit = unsupported("write")

    override def moveTo(target: String, force: Boolean): Unit = unsupported("write")

    // TODO: generalise to all URIResolvers! Renamed to treeCopyTo
    def treeExtractTo(targetRootExe: URIResolver#Execution, mode: WriteMode): Unit = inputNoValidation { i =>
      val offspring = i.offspring
      offspring.foreach { v: ClasspathResolver#Execution =>
        val dst = CommonUtils.\\\(targetRootExe.absolutePathStr, v.absolutePathStr)

        v.copyTo(targetRootExe.outer.execute(dst), mode)
      }
      Thread.sleep(5000) //for eventual consistency
    }

    override def cleanImpl(): Unit = {

      referenceOpt.foreach { v =>
        v.close()
      }
      super.cleanImpl()
    }
  }

  def overview: Overview = Overview()

  case class Overview(
      pathConflictFilter: String => Boolean = { v =>
        val exceptions = Set.empty[String]
        v.endsWith(".class") && (!exceptions.contains(v))
      }
  ) extends UseScanResult {

    def debugConfFiles(
        fileNames: Seq[String] = List(
          "log4j.properties",
          "rootkey.csv",
          ".rootkey.csv"
        )
    ): Unit = {

      {
        val resolvedInfos = fileNames.map { v =>
          _Execution(v).referenceInfo
        }
        println("resolving files in classpath ...\n" + resolvedInfos.mkString("\n"))
      }
    }

    object Files {

      lazy val paths: Seq[String] = {
        elementsOverride.getOrElse {
          scanned.getClasspathURIs.asScala.toList.map(_.toString)
        }
      }

      lazy val prunedPaths: Seq[String] = {

        val pruned = paths.map { v =>
          prunePath(v)
        }
        val sorted = pruned.sorted
        sorted

      }

      lazy val names: Seq[String] = {
        val name = paths.flatMap { v =>
          StringUtils.split(v, File.separator).lastOption
        }
        val sorted = name.sorted
        sorted
      }

      lazy val formatted: String = names.mkString("\n")
    }

    object Conflicts {

      lazy val raw: Map[String, List[String]] = {
        val seen = mutable.Map.empty[String, mutable.ArrayBuffer[String]]
        try {

          val resources = scanned.getAllResources.asScala
          for (resource <- resources) {
            val path = resource.getPath
            val classpathElements = seen.getOrElseUpdate(path, mutable.ArrayBuffer.empty)

            classpathElements += resource.getClasspathElementFile.getPath
          }
        }

        val result = seen.toMap.flatMap {
          case (k, v) =>
            if (!pathConflictFilter(k)) None
            else if (v.size == 1) None
            else Some(k -> v.toList.sorted)
        }

        result
      }

      lazy val aggregated: Map[List[String], List[String]] = raw.toList
        .groupBy {
          case (_, vs) =>
            vs
        }
        .map { kvs =>
          kvs._2.map(_._1).sorted -> kvs._1
        }

      lazy val formatted: String = {

        val info = aggregated
          .map {
            case (k, v) =>
              s"""
                 |${k.mkString("", "\n", "")}:
                 |${v.mkString("\t", "\n\t", "")}
                 |""".stripMargin.trim
          }
          .mkString("\n\n")
        info
      }
    }

    lazy val fullReport: String =
      s"""
         | === CONFLICTS ===
         |${Conflicts.formatted}
         | 
         | === FILES ====
         |${Files.formatted}
         |""".stripMargin.trim
  }

}

object ClasspathResolver {

  object default extends ClasspathResolver()

  // TODO: remove, should use default
//  object system extends ClasspathResolver(classLoaderOverride = Some(ClassLoader.getSystemClassLoader))

  class ForSparkEnv(info: String) extends ClasspathResolver {
    val elements: Seq[String] = {
      val rows = info.split('\n').filter(_.nonEmpty)

      val paths = rows.map(v => v.split('\t').head)
      paths.toSeq
    }
    ClasspathResolver(
      elementsOverride = Some(elements)
    )
  }

  implicit def toDefault(v: this.type): ClasspathResolver = v.default

  def main(args: Array[String]): Unit = {

    val run = default.overview
    println(run.fullReport)
  }
}
