package com.tribbloids.spookystuff.actions

import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.actions.HasTrace.NoStateChange
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Error.ValidationError
import com.tribbloids.spookystuff.doc.{Doc, Observation}

trait Foundation extends Serializable {

  trait HasTraceSet {

    def traceSet: Set[Trace]

    def *>(that: HasTraceSet): Set[Trace] = {
      val newTraces = this.traceSet.flatMap(left =>
        that.traceSet.map { right =>
          left +> right
        }
      )
      newTraces.map(v => v: Trace)
    }

    def ||(other: HasTraceSet): Set[Trace] = traceSet ++ other.traceSet
  }

  type DocFilter = DocFilter.Lemma

  case object DocFilter {

    val domain: Hom.Fn.BuildDomains[(Doc, Agent), Doc] = Hom.Fn.at[(Doc, Agent)].to[Doc]

    type Lemma = domain._Lemma

    lazy val default: DocFilter.MustHaveTitle.type = DocFilter.MustHaveTitle
    lazy val defaultForImage: DocFilter.AcceptStatusCode2XX.type = DocFilter.AcceptStatusCode2XX

    // TODO: support chaining & extends ExpressionLike/TreeNode
    sealed trait Impl extends domain._Impl {

      def assertStatusCode(page: Doc): Unit = {
        page.httpStatus.foreach { v =>
          assert(v.getStatusCode.toString.startsWith("2"), v.toString)
        }
      }

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

    case object AcceptStatusCode2XX extends Impl {

      override def applyNoErrorDump(v: (Doc, Agent)): Doc = {
        val result = v._1
        assertStatusCode(result)
        result
      }
    }

    case object MustHaveTitle extends Impl {

      override def applyNoErrorDump(v: (Doc, Agent)): Doc = {
        val doc = v._1
        assertStatusCode(doc)
        if (doc.mimeType.contains("html")) {
          if (doc.root.\("html").\("title").text.getOrElse("").isEmpty) {

            throw ValidationError(
              s"Html Page @ ${doc.uri} has no title",
              doc
            )
          }
        }
        doc
      }
    }
  }

  case object NoOp extends HasTrace with NoStateChange {
    override def trace: Trace = Trace(Nil)

    override def apply(agent: Agent): Seq[Observation] = Nil
  }
}
