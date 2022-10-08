package com.tribbloids.spookystuff.utils.classpath

import com.tribbloids.spookystuff.utils.io.{LocalResolver, WriteMode}
import com.tribbloids.spookystuff.utils.{CommonConst, CommonUtils}
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class ClasspathResolverSpec extends AnyFunSpec {

  describe("copyResourceToDirectory") {

    it("can extract a dependency's package in a jar") {
      val dst = CommonUtils.\\\(CommonConst.USER_TEMP_DIR, "log4j")

      ClasspathResolver
        .execute("org/apache/log4j/xml")
        .treeExtractTo(LocalResolver.execute(dst), WriteMode.Overwrite)

      val dir = new File(dst)
      assert(dir.list().nonEmpty)
    }

    it("can extract a package in file system") {

      val dst = CommonUtils.\\\(CommonConst.USER_TEMP_DIR, "utils")

      ClasspathResolver
        .execute("com/tribbloids/spookystuff/utils/io/lock")
        .treeExtractTo(LocalResolver.execute(dst), WriteMode.Overwrite)

      val dir = new File(dst)
      assert(dir.list().nonEmpty)
    }
  }
}