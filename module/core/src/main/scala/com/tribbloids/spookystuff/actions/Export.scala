package com.tribbloids.spookystuff.actions

import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.actions.HasTrace.NoStateChange
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.*
import com.tribbloids.spookystuff.doc.Error.ValidationError

/**
  * Export a page from the browser or http client the page an be anything including HTML/XML file, image, PDF file or
  * JSON string.
  */
@SerialVersionUID(564570120183654L)
abstract class Export extends MayExport.Named with NoStateChange {

  import Export.*

  override def exportNames: Set[String] = Set(name)

  def doExe(agent: Agent): Seq[Observation]

  def accept(validation: DocValidation): Export.Accept = {

    Export.Accept(this, validation)
  }
}

object Export {

  type DocValidation = DocValidation.Lemma

  case object DocValidation {

    val domain: Hom.Fn.DomainBuilder[(Doc, Agent), Doc] = Hom.Fn.at[(Doc, Agent)].to[Doc]

    type Lemma = domain._Lemma

    lazy val default: DocValidation.HasTitle.type = DocValidation.HasTitle
    lazy val defaultForImage: DocValidation.StatusCode2XX.type = DocValidation.StatusCode2XX

    // TODO: support chaining & extends ExpressionLike/TreeNode
    sealed trait Impl extends domain._Impl {

      final override def apply(v: (Doc, Agent)): Doc = {

        applyNoErrorDump(v)
      }

      def applyNoErrorDump(v: (Doc, Agent)): Doc
    }

    case object Bypass extends Impl {

      override def applyNoErrorDump(v: (Doc, Agent)): Doc = {
        v._1
      }
    }

    case object StatusCode2XX extends Impl {

      private def assertStatusCode(page: Doc): Unit = {
        page.httpStatus.foreach { v =>
          assert(v.getStatusCode.toString.startsWith("2"), v.toString)
        }
      }

      override def applyNoErrorDump(v: (Doc, Agent)): Doc = {
        val result = v._1
        assertStatusCode(result)
        result
      }
    }

    case object HasTitle extends Impl {

      override def applyNoErrorDump(v: (Doc, Agent)): Doc = {
        val doc = StatusCode2XX.applyNoErrorDump(v)

        if (doc.mimeType.contains("html")) {
          if (doc.root.\("html").\("title").texts.mkString("\n").isEmpty) {

            throw ValidationError(
              s"Looking for <html>/<title> @ ${doc.uri} but can't find any",
              doc
            )
          }
        }
        doc
      }
    }

    case object HasHead extends Impl {

      override def applyNoErrorDump(v: (Doc, Agent)): Doc = {
        val doc = StatusCode2XX.applyNoErrorDump(v)

        if (doc.mimeType.contains("html")) {
          if (doc.root.\("html").\("head").texts.mkString("\n").isEmpty) {

            throw ValidationError(
              s"Looking for <html>/<head> @ ${doc.uri} but can't find any",
              doc
            )
          }
        }
        doc
      }
    }
  }

  case class Accept(
      original: Export,
      validation: DocValidation
  ) extends Export {

    override def doExe(agent: Agent): Seq[Observation] = {

      val results = original.doExe(agent)
      results.map {
        case doc: Doc =>
          val result = validation.apply(doc -> agent)
          result

        case other: Observation =>
          other
      }
    }

    override protected def originalName: String = original.name
  }
}
