package com.tribbloids.spookystuff.utils.classpath

import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.utils.io.Resource.{DIR, FILE}
import com.tribbloids.spookystuff.utils.io.{
  Resource => IOResource,
  ResourceMetadata,
  URIExecution,
  URIResolver,
  WriteMode
}
import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import io.github.classgraph.{ClassGraph, Resource, ResourceList, ScanResult}
import org.apache.spark.ml.dsl.utils.LazyVar

import java.io.{File, IOException, InputStream, OutputStream}
import java.nio.file.{Files, NoSuchFileException}
import java.util.regex.Pattern
import scala.language.implicitConversions

case class ClasspathResolver(
    elementsOverride: Option[Seq[String]] = None,
    classLoaderOverride: Option[ClassLoader] = None
) extends URIResolver {

  import scala.jdk.CollectionConverters._

  @transient lazy val metadataParser: ResourceMetadata.ReflectionParser[Resource] =
    ResourceMetadata.ReflectionParser[Resource]()

  lazy val graph: ClassGraph = {
    var base = new ClassGraph() // .enableClassInfo.ignoreClassVisibility

    elementsOverride.foreach { oo =>
      base = base.overrideClasspath(oo)
    }

    classLoaderOverride.foreach { oo =>
      base = base.overrideClassLoaders(oo)
    }

    base
  }

  trait Scanning extends Cleanable {

    // TODO: this may not be efficient as every new _Execution requires a new can, but for safety ...
    lazy val _scanResult: LazyVar[ScanResult] = LazyVar {
      graph.scan()
    }

    def scanResult: ScanResult = _scanResult.value

    override def cleanImpl(): Unit = {
      _scanResult.peek.foreach { v =>
        v.close()
      }
    }
  }

//  object _Execution extends (String => _Execution) {}

  case class _Execution(
      pathStr: String
  ) extends Execution {

    lazy val childPattern: String = CommonUtils.\\\(pathStr, "*")
    lazy val offspringPattern: String = CommonUtils.\\\(pathStr, "**")

    lazy val referenceInfo: String = io() { io =>
      val info = io.firstRefOpt match {
        case Some(r) =>
          s"\tresource `$pathStr` refers to ${r.getURL.toString}"
        case None =>
          s"\tresource `$pathStr` has no reference"
      }
      info
    }

    override def absolutePathStr: String = pathStr

    case class _Resource(mode: WriteMode) extends IOResource with Scanning {

      override protected def _outer: URIExecution = _Execution.this

      lazy val _refs: LazyVar[ResourceList] = LazyVar {
        scanResult.getResourcesWithPath(pathStr)
      }
      def firstRefOpt: Option[Resource] = _refs.value.asScala.headOption
      def firstRef: Resource = firstRefOpt.getOrElse(???)

      override lazy val getURI: String = firstRef.getURI.toString

      override def getName: String = firstRef.getURI.getFragment

      override def getType: String =
        if (!_refs.value.isEmpty) FILE
        else if (children.nonEmpty) DIR
        else throw new NoSuchFileException(s"File $pathStr doesn't exist")

      def find(wildcard: String): Seq[_Execution] = {
        val list = scanResult
          .getResourcesMatchingWildcard(wildcard)
          .asScala
          .map { v =>
            v.getPath
          }
          .distinct

        val result = list.map { v =>
          _Execution(v)
        }
        result.toSeq
      }

      override lazy val children: Seq[_Execution] = find(childPattern)
      lazy val offspring: Seq[_Execution] = find(offspringPattern)

      override def getContentType: String = Files.probeContentType(firstRef.getClasspathElementFile.toPath)

      override def getLength: Long = firstRef.getLength

      override def getLastModified: Long = firstRef.getLastModified

      override protected def _metadata: ResourceMetadata = metadataParser(firstRef)

      override protected def _newIStream: InputStream = {
        try {
          firstRef.open()
        } catch {
          case e: Throwable =>
            throw new IOException(
              s"cannot read $getURI",
              e
            )
        }
      }

      override protected def _newOStream: OutputStream = unsupported("write")

      override def cleanImpl(): Unit = {

        _refs.peek.foreach { v =>
          v.close()
        }
        super.cleanImpl()
      }
    }

    override protected def _delete(mustExist: Boolean): Unit = unsupported("write")

    override def moveTo(target: String, force: Boolean): Unit = unsupported("write")

    def treeCopyTo(targetRootExe: URIResolver#Execution, mode: WriteMode): Unit = io() { i =>
      val offspring = i.offspring
      offspring.foreach { v: ClasspathResolver#Execution =>
        val dst = CommonUtils.\\\(targetRootExe.absolutePathStr, v.absolutePathStr)

        v.copyTo(targetRootExe.outer.execute(dst), mode)
      }
      Thread.sleep(5000) // for eventual consistency
    }

  }

  def debug[T](fn: Debugging => T): T = {
    val o = Debugging()

    try {
      fn(o)
    } finally {
      o.clean()
    }
  }

  case class Debugging() extends Scanning {

    case class Display(
        pathConflictFilter: String => Boolean = { v =>
          val exceptions = Set.empty[String]
          v.endsWith(".class") &&
          (!v.startsWith("module-info")) &&
          (!exceptions.contains(v))
        },
        pathFormatting: String => String = identity
    ) {

      object Files {

        lazy val paths: Seq[String] = {
          elementsOverride.getOrElse {
            scanResult.getClasspathURIs.asScala.toList.map(_.toString)
          }
        }

        lazy val displayPath: Seq[String] = {

          val pruned = paths.map { v =>
            pathFormatting(v)
          }
          val sorted = pruned.sorted
          sorted
        }

        lazy val formatted: String = displayPath.mkString("\n")

        def debugConfs(
            fileNames: Seq[String] = List(
              "log4j.properties",
              "log4j2.properties",
              "rootkey.csv",
              ".rootkey.csv"
            )
        ): Unit = {

          val resolvedInfos = fileNames.map { v =>
            _Execution(v).referenceInfo
          }
          println("resolving files in classpath ...\n" + resolvedInfos.mkString("\n"))
        }
      }

      object Conflicts {

        lazy val raw: Map[String, List[String]] = {
          val seen =
            try {

              val resources = scanResult.getAllResources.asScala

              val result = resources
                .groupBy { resource =>
                  resource.getPath
                }
                .map {
                  case (k, vs) =>
                    k -> vs.map { v =>
                      val filePath = v.getClasspathElementURI.toString
                      pathFormatting(filePath)
                    }
                }
              result
            }

          val result = seen.flatMap {
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

      lazy val completeReport: String =
        s"""
             | === CONFLICTS ===
             |${Conflicts.formatted}
             | 
             | === FILES ====
             |${Files.formatted}
             |""".stripMargin.trim
    }

    lazy val default: Display = Display()

    lazy val fileNameOnly: Display = Display(pathFormatting = ClasspathResolver.getName)
  }

}

object ClasspathResolver {

  object System extends ClasspathResolver(classLoaderOverride = Some(ClassLoader.getSystemClassLoader))

  object AllInclusive extends ClasspathResolver()

  lazy val default: ClasspathResolver = AllInclusive

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

  lazy val localRepoRoot: Seq[String] = Seq(
    ".m2/repository",
    ".gradle/caches",
    "jre/lib",
    "jdk/lib"
  )

  def getLocalRepoPath(v: String): String = {

    for (root <- localRepoRoot) {

      if (v.contains(root))
        return v.split(Pattern.quote(root)).last
    }

    v
  }

  def getName(v: String): String = {

    v.split(File.separator)
      .lastOption
      .getOrElse(s"file path $v is empty")
  }
}
