package com.tribbloids.spookystuff.pipeline

import com.tribbloids.spookystuff.pipeline.transformer.dbpedia.LookupTransformer
import com.tribbloids.spookystuff.pipeline.transformer.google.{ImageSearchTransformer, WebSearchTransformer}

/**
 * Created by peng on 27/10/15.
 */
class TestAllTransformers extends SpookyEnvSuite {

  val transformers = Seq(
    new ImageSearchTransformer(),
    new WebSearchTransformer(),
    new LookupTransformer()
  )

  for (transformer <- transformers) {
    test(transformer.getClass.getCanonicalName) {
      transformer.test(spooky)
    }
  }
}