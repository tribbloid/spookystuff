package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.testutils.TestHelper
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class SCFunctionsSuite extends FunSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = {}

  TestHelper.TestSC

  describe("withJob") {

    it("can stack description") {

      SCFunctions.withJob("aaa") {

        SCFunctions.withJob("bbb") {

          TestHelper.TestSC.parallelize(1 to 100).map(v => v * v).collect()

          assert(SCFunctions.scLocalProperties.description == "aaa \u2023 bbb")
        }
      }
    }

    it("will not override existing groupID if not specified") {

      SCFunctions.withJob("aaa", "aaa") {

        SCFunctions.withJob("bbb") {

          TestHelper.TestSC.parallelize(1 to 100).map(v => v * v).collect()

          assert(SCFunctions.scLocalProperties.groupID == "aaa")
        }
      }
    }

    it("can override existing groupID") {

      SCFunctions.withJob("aaa", "aaa") {

        SCFunctions.withJob("bbb", "bbb") {

          val ugi = UserGroupInformation.createRemoteUser("ccc")

          TestHelper.TestSC.parallelize(1 to 100).map(v => v * v).collect()

          assert(SCFunctions.scLocalProperties.groupID == "bbb")
        }
      }
    }
  }

  override def afterAll(): Unit = {

//    SCFunctions.blockUntilKill(99999)
  }
}
