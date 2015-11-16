package com.tribbloids.spookystuff.pipeline

/**
  * Created by peng on 27/10/15.
  */
class TestAllTransformers extends SpookyEnvSuite {

  val transformers = Seq(
    new google.ImageSearchTransformer(),
    new google.WebSearchTransformer(),
    new dbpedia.LookupTransformer(),
    new dbpedia.RelationTransformer(),
    new mymemory.TranslationTransformer(),
    new alchemyapi.GetKeywordsTransformer()
  )

  for (transformer <- transformers) {
    test(transformer.getClass.getCanonicalName) {
      transformer.test(spooky)
    }
  }
}