package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import com.tribbloids.spookystuff.doc.{Doc, Observation}

package object conf {

  type Auditing = Auditing.build._Lemma {}

  object Auditing extends Enumeration {

    val build: :=>.BuildDomains[Observation, Seq[Doc]] = :=>.at[Observation].to[Seq[Doc]]

    val Disabled: build._Impl = build { _ =>
      Seq.empty
    }

    val Original: build._Impl = build { v1 =>
      v1.docForAuditing.toSeq
    }

    val Converted: build._Impl = build { v1 =>
      v1.docForAuditing.flatMap(_.normalised.docForAuditing).toSeq
    }

    val Both: build._Impl = build { v1 =>
      (Original(v1) ++ Converted(v1)).distinct
    }
  }

}
