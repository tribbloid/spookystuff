package com.tribbloids.spookystuff.conf

import ai.acyclic.prover.commons.function.Impl
import com.tribbloids.spookystuff.doc.{Doc, Observation}

trait Auditing extends Impl.Fn[Observation, Seq[Doc]] {}

object Auditing extends Enumeration {

  object Disabled extends Auditing {
    override def apply(v1: Observation): Seq[Doc] = Seq.empty
  }

  object Original extends Auditing {
    override def apply(v1: Observation): Seq[Doc] = v1.docForAuditing.toSeq
  }

  object Converted extends Auditing {
    override def apply(v1: Observation): Seq[Doc] = v1.docForAuditing.flatMap(_.normalised.docForAuditing).toSeq
  }

  object Both extends Auditing {
    override def apply(v1: Observation): Seq[Doc] = {

      (Original(v1) ++ Converted(v1)).distinct
    }
  }
}
