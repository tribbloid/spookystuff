package com.tribbloids.spookystuff.commons.classpath

import ai.acyclic.prover.commons.spark.Envs
import com.tribbloids.spookystuff.commons.classpath.ClasspathResolver
import com.tribbloids.spookystuff.io.{LocalResolver, WriteMode}
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class ClasspathResolverSpec extends AnyFunSpec {

  describe("copyResourceToDirectory") {

    it("can extract a dependency's package in a jar") {
      val dst = Envs.USER_TEMP_DIR \\ "log4j"

      ClasspathResolver
        .execute("org/apache/log4j/xml")
        .treeCopyTo(LocalResolver.execute(dst.universal), WriteMode.Overwrite)

      val dir = new File(dst)
      assert(dir.list().nonEmpty)
    }

    it("can extract a package in file system") {

      val dst = Envs.USER_TEMP_DIR \\ "utils"

      ClasspathResolver
        .execute("com/tribbloids/spookystuff/io/lock")
        .treeCopyTo(LocalResolver.execute(dst.universal), WriteMode.Overwrite)

      val dir = new File(dst)
      assert(dir.list().nonEmpty)
    }
  }
}
