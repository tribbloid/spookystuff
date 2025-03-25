package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import com.tribbloids.spookystuff.doc.{Doc, Observation}

package object conf {

  type Auditing = Auditing.fn._Lemma {}

  object Auditing extends Enumeration {

    val fn: :=>.BuildDomains[Observation, Seq[Doc]] = :=>.at[Observation].to[Seq[Doc]]

    val disabled: fn._Impl = fn { _ =>
      Seq.empty
    }

    val original: fn._Impl = fn { v1 =>
      v1.docForAuditing.toSeq
    }

    val normalised: fn._Impl = fn { v1 =>
      v1.docForAuditing.flatMap(_.converted.docForAuditing).toSeq
    }

    val originalAndNormalised: fn._Impl = fn { v1 =>
      (original(v1) ++ normalised(v1)).distinct
    }
  }

}
