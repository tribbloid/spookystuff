package com.tribbloids.spookystuff.commons.classpath

import ai.acyclic.prover.commons.util.{LazyVar, PathMagnet}
import com.tribbloids.spookystuff.commons.data.ReflCanUnapply
import com.tribbloids.spookystuff.commons.lifespan.Cleanable
import com.tribbloids.spookystuff.io.{Resource, ResourceMetadata, URIExecution, URIResolver, WriteMode}
import io.github.classgraph.{ClassGraph, ResourceList, ScanResult}

import java.io.{File, IOException, InputStream, OutputStream}
import java.nio.file.{Files, NoSuchFileException}
import java.util.regex.Pattern
import scala.language.implicitConversions

case class ClasspathResolver(
    elementsOverride: Option[Seq[String]] = None,
    classLoaderOverride: Option[ClassLoader] = None
) extends URIResolver {

  import scala.jdk.CollectionConverters.*
  import ClasspathResolver.*

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

  trait CanScan extends Cleanable {

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

  implicit class _Execution(
      originalPath: PathMagnet.URIPath
  ) extends Execution {

    lazy val path = originalPath.normaliseToLocal

    lazy val referenceInfo: String = doIO() { io =>
      val info = io.firstRefOpt match {
        case Some(r) =>
          s"\tresource `$path` refers to ${r.getURL.toString}"
        case None =>
          s"\tresource `$path` has no reference"
      }
      info
    }

    override def absolutePath: PathMagnet.URIPath =
      PathMagnet.URIPath(path) // TODO: classpath jar file should use absolute path

    object _Resource extends (WriteMode => _Resource)
    case class _Resource(mode: WriteMode) extends Resource with CanScan {

      override protected def _outer: URIExecution = _Execution.this

      lazy val _refs: LazyVar[ResourceList] = LazyVar {
        val v = scanResult.getResourcesWithPath(path)
        v
      }
      def firstRefOpt: Option[io.github.classgraph.Resource] = _refs.value.asScala.headOption
      def firstRef: io.github.classgraph.Resource = firstRefOpt.getOrElse(???)

      override lazy val getURI: String = firstRef.getURI.toString

      override def getName: String = firstRef.getURI.getFragment

      override def getType: String =
        if (!_refs.value.isEmpty) Resource.FILE
        else if (children.nonEmpty) Resource.DIR
        else throw new NoSuchFileException(s"File $path doesn't exist")

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

      override lazy val children: Seq[_Execution] = find(path.glob_children)
      lazy val offspring: Seq[_Execution] = find(path.glob_offspring)

      override def getContentType: String = Files.probeContentType(firstRef.getClasspathElementFile.toPath)

      override def getLength: Long = firstRef.getLength

      override def getLastModified: Long = firstRef.getLastModified

      override protected def extraMetadata: ResourceMetadata = {

        val unapplied = unapplyResource.unapply(firstRef)
        ResourceMetadata.BuildFrom.unappliedForm(unapplied)
      }

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

    def treeCopyTo(targetRootExe: URIResolver#Execution, mode: WriteMode): Unit = doIO() { i =>
      val offspring = i.offspring
      offspring.foreach { (v: ClasspathResolver#Execution) =>
        val dst = targetRootExe.absolutePath :/ v.absolutePath

        v.copyTo(targetRootExe.outer.on(dst), mode)
      }
      Thread.sleep(5000) // for eventual consistency
    }

  }

  def debug[T](fn: CanDebug => T): T = {
    val o = CanDebug()

    try {
      fn(o)
    } finally {
      o.clean()
    }
  }

  case class CanDebug() extends CanScan {

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
          val seen = {

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

  lazy val unapplyResource: ReflCanUnapply[io.github.classgraph.Resource] =
    ReflCanUnapply[io.github.classgraph.Resource]()
}
