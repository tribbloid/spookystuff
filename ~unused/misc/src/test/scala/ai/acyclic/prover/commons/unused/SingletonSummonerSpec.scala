package ai.acyclic.prover.commons.unused

import ai.acyclic.prover.commons.testlib.BaseSpec

class SingletonSummonerSpec extends BaseSpec {

  it("without") {
    object S0 extends AnyRef

    implicitly[ValueOf[S0.type]]

    shouldNotCompile(
      "implicitly[S0.type]"
    )
  }

  it("with") {
    object S1 extends SingletonSummoner

    implicitly[S1.type]
  }
}
