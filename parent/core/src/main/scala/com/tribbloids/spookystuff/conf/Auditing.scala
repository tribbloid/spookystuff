package com.tribbloids.spookystuff.conf

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import com.tribbloids.spookystuff.doc.{Doc, Observation}

trait Auditing extends :=>[Observation, Seq[Doc]] {}

object Auditing extends Enumeration {

  case object Disabled extends Auditing {
    override def apply(v1: Observation): Seq[Doc] = Seq.empty
  }

  case object Original extends Auditing {
    override def apply(v1: Observation): Seq[Doc] = v1.docForAuditing.toSeq
  }

  case object Converted extends Auditing {
    override def apply(v1: Observation): Seq[Doc] = v1.docForAuditing.flatMap(_.normalised.docForAuditing).toSeq
  }

  case object Both extends Auditing {
    override def apply(v1: Observation): Seq[Doc] = {

      (Original(v1) ++ Converted(v1)).distinct
    }
  }
}
