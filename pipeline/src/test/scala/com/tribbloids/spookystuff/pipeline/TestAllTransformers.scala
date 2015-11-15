package com.tribbloids.spookystuff.pipeline

import com.tribbloids.spookystuff.pipeline.mymemory.TranslationTransformer

/**
  * Created by peng on 27/10/15.
  */
class TestAllTransformers extends SpookyEnvSuite {

  val transformers = Seq(
    new google.ImageSearchTransformer(),
    new google.WebSearchTransformer(),
    new dbpedia.LookupTransformer(),
    new dbpedia.RelationTransformer(),
    new TranslationTransformer()
  )

  for (transformer <- transformers) {
    test(transformer.getClass.getCanonicalName) {
      transformer.test(spooky)
    }
  }
}