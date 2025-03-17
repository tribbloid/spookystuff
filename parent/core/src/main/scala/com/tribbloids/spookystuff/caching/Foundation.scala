package com.tribbloids.spookystuff.caching

import com.tribbloids.spookystuff.actions.{Action, Trace}

trait Foundation {

  sealed trait CacheKey {

    final def lastAction: Action = shortestForm.last

    def shortestForm: Trace

    def contains(trace: Trace): Boolean
  }

  object CacheKey {

    case class NormalFormKey(
        form: Trace
    ) extends CacheKey {

      override def shortestForm: Trace = form

      override def contains(trace: Trace): Boolean = form == trace
    }

    case class EqFormKey(
        forms: Set[Trace]
    ) extends CacheKey {

      // an E-class in e-graph identified by multiple Traces all evaluates to the same results
      // used only if optimal normal form doesn't exist
      // TODO: not used at the moment

      override def shortestForm: Trace = forms.minBy(_.length)

      override def contains(trace: Trace): Boolean = forms.contains(trace)
    }
  }
}
